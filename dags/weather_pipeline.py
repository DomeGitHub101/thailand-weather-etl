from datetime import date, datetime, timedelta

import pendulum

from airflow.sdk import Param, dag, get_current_context, task


CITIES = [
    {"code": "bangkok", "latitude": 13.7563, "longitude": 100.5018},
    {"code": "chiang_mai", "latitude": 18.7883, "longitude": 98.9853},
    {"code": "khon_kaen", "latitude": 16.4322, "longitude": 102.8236},
    {"code": "phuket", "latitude": 7.8804, "longitude": 98.3923},
    {"code": "hat_yai", "latitude": 7.0084, "longitude": 100.4747},
]


@dag(
    dag_id="weather_pipeline",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Bangkok"),
    schedule="0 8 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    params={
        "start_date": Param(
            None,
            type=["null", "string"],
            format="date",
            description="First date, YYYY-MM-DD; leave empty for automatic mode",
        ),
        "end_date": Param(
            None,
            type=["null", "string"],
            format="date",
            description="Last date, YYYY-MM-DD; leave empty for automatic mode",
        ),
    },
    tags=["weather", "etl"],
)
def weather_pipeline():

    @task
    def resolve_window():
        context = get_current_context()
        params = context["params"]

        start_text = params["start_date"]
        end_text = params["end_date"]

        if bool(start_text) != bool(end_text):
            raise ValueError("Provide both dates, or leave both empty")

        if start_text and end_text:
            start = date.fromisoformat(start_text)
            end = date.fromisoformat(end_text)
        else:
            # ใช้เวลาของ run เดิม เพื่อให้ retry เลือกวันเดิม
            anchor = context.get("data_interval_end")

            if anchor is None:
                raise ValueError(
                    "This run has no data interval. Provide both dates."
                )

            target = (
                pendulum.instance(anchor)
                .in_timezone("Asia/Bangkok")
                .subtract(days=7)
                .date()
            )
            start = end = target

        days = (end - start).days + 1

        # จำกัดข้อมูลที่ส่งผ่าน XCom สำหรับเวอร์ชันฝึก
        if not 1 <= days <= 7:
            raise ValueError("Choose a date range of 1 to 7 days")

        window = {
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "expected_hours": days * 24,
        }

        print(f"Selected window: {window}")
        return window

    @task
    def extract_city(city, window):
        from weather_etl.extract import extract_weather

        return extract_weather(
            latitude=city["latitude"],
            longitude=city["longitude"],
            start_date=window["start_date"],
            end_date=window["end_date"],
        )

    @task
    def transform_city(payload, city, window):
        from weather_etl.transform import transform_weather

        records = transform_weather(payload, city["code"])

        if len(records) != window["expected_hours"]:
            raise ValueError(
                f"{city['code']}: expected {window['expected_hours']} "
                f"records, received {len(records)}"
            )

        for record in records:
            record["observed_at"] = record["observed_at"].isoformat()

        return records

    @task
    def load_city(records):
        from weather_etl.load import load_weather

        database_records = [
            {
                **record,
                "observed_at": datetime.fromisoformat(
                    record["observed_at"]
                ),
            }
            for record in records
        ]

        return load_weather(database_records)

    @task
    def validate_city(processed_count, city, window):
        import os

        import psycopg

        start = pendulum.parse(
            window["start_date"],
            tz="Asia/Bangkok",
        )
        end = pendulum.parse(
            window["end_date"],
            tz="Asia/Bangkok",
        ).add(days=1)

        expected_count = window["expected_hours"]

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
                    (city["code"], start, end),
                )
                rows = cursor.fetchall()

        if processed_count != expected_count or len(rows) != expected_count:
            raise ValueError(
                f"{city['code']}: expected={expected_count}, "
                f"processed={processed_count}, stored={len(rows)}"
            )

        expected_times = {
            start.add(hours=hour)
            for hour in range(expected_count)
        }
        actual_times = {row[0] for row in rows}

        if actual_times != expected_times:
            raise ValueError(
                f"{city['code']}: missing or misaligned timestamps"
            )

        for observed_at, temperature, humidity, rain in rows:
            if any(
                value is None
                for value in (temperature, humidity, rain)
            ):
                raise ValueError(f"Missing value at {observed_at}")

            if not 0 <= humidity <= 100:
                raise ValueError(f"Invalid humidity at {observed_at}")

            if rain < 0:
                raise ValueError(f"Negative precipitation at {observed_at}")

        print(
            f"Validation passed: {city['code']}, "
            f"{window['start_date']} to {window['end_date']}, "
            f"{len(rows)} rows"
        )

    window = resolve_window()

    for city in CITIES:
        code = city["code"]

        raw = extract_city.override(
            task_id=f"extract_{code}"
        )(city, window)

        clean = transform_city.override(
            task_id=f"transform_{code}"
        )(raw, city, window)

        count = load_city.override(
            task_id=f"load_{code}"
        )(clean)

        validate_city.override(
            task_id=f"validate_{code}"
        )(count, city, window)


weather_pipeline()