from datetime import datetime, timedelta

import pendulum

from airflow.sdk import dag, task


CITY_CODE = "bangkok"
TARGET_DATE = "2025-01-01"


@dag(
    dag_id="weather_pipeline",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Bangkok"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["weather", "etl"],
)
def weather_pipeline():

    @task
    def extract_weather():
        from weather_etl.extract import extract_weather as extract

        payload = extract(
            latitude=13.7563,
            longitude=100.5018,
            start_date=TARGET_DATE,
            end_date=TARGET_DATE,
        )

        print(f"Extracted {len(payload['hourly']['time'])} hours")
        return payload

    @task
    def transform_weather(payload):
        from weather_etl.transform import transform_weather as transform

        records = transform(payload, CITY_CODE)

        if len(records) != 24:
            raise ValueError(
                f"Expected 24 records, received {len(records)}"
            )

        # ส่งเวลาเป็นข้อความ ISO เพื่อส่งต่อระหว่าง task
        for record in records:
            record["observed_at"] = record["observed_at"].isoformat()

        print(f"Transformed {len(records)} records")
        return records

    @task
    def load_weather(records):
        from weather_etl.load import load_weather as load

        # แปลงกลับเป็น datetime ก่อนส่งให้ psycopg
        database_records = [
            {
                **record,
                "observed_at": datetime.fromisoformat(
                    record["observed_at"]
                ),
            }
            for record in records
        ]

        count = load(database_records)
        print(f"Loaded or updated {count} records")
        return count

    @task
    def validate_loaded_data(processed_count):
        import os

        import psycopg

        start = pendulum.parse(
            TARGET_DATE,
            tz="Asia/Bangkok",
        )
        end = start.add(days=1)

        with psycopg.connect(
            host=os.environ["WAREHOUSE_HOST"],
            port=int(os.environ["WAREHOUSE_PORT"]),
            dbname=os.environ["WAREHOUSE_DB"],
            user=os.environ["WAREHOUSE_USER"],
            password=os.environ["WAREHOUSE_PASSWORD"],
            connect_timeout=10,
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        observed_at,
                        temperature_c,
                        relative_humidity_pct,
                        precipitation_mm
                    FROM weather_hourly
                    WHERE city_code = %s
                      AND observed_at >= %s
                      AND observed_at < %s
                    ORDER BY observed_at
                    """,
                    (CITY_CODE, start, end),
                )
                rows = cursor.fetchall()

        if processed_count != 24 or len(rows) != 24:
            raise ValueError(
                f"Expected 24 records: processed={processed_count}, "
                f"stored={len(rows)}"
            )

        expected_times = {
            start.add(hours=hour)
            for hour in range(24)
        }
        actual_times = {row[0] for row in rows}

        if actual_times != expected_times:
            raise ValueError(
                "Hourly timestamps are missing or misaligned"
            )

        for observed_at, temperature, humidity, rain in rows:
            if any(
                value is None
                for value in (temperature, humidity, rain)
            ):
                raise ValueError(
                    f"Missing weather value at {observed_at}"
                )

            if not 0 <= humidity <= 100:
                raise ValueError(
                    f"Invalid humidity at {observed_at}: {humidity}"
                )

            if rain < 0:
                raise ValueError(
                    f"Negative precipitation at {observed_at}: {rain}"
                )

        print(
            f"Validation passed: {CITY_CODE} on {TARGET_DATE}; "
            "24 hourly timestamps, no missing values, "
            "humidity and precipitation checks passed"
        )

    raw_data = extract_weather()
    clean_data = transform_weather(raw_data)
    processed_count = load_weather(clean_data)
    validate_loaded_data(processed_count)


weather_pipeline()