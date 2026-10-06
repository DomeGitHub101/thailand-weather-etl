import os

import psycopg


UPSERT_SQL = """
INSERT INTO weather_hourly (
    city_code,
    observed_at,
    temperature_c,
    relative_humidity_pct,
    precipitation_mm
)
VALUES (
    %(city_code)s,
    %(observed_at)s,
    %(temperature_c)s,
    %(relative_humidity_pct)s,
    %(precipitation_mm)s
)
ON CONFLICT (city_code, observed_at)
DO UPDATE SET
    temperature_c = EXCLUDED.temperature_c,
    relative_humidity_pct = EXCLUDED.relative_humidity_pct,
    precipitation_mm = EXCLUDED.precipitation_mm,
    loaded_at = NOW();
"""


def load_weather(records):
    if not records:
        raise ValueError("No records to load")

    password = os.getenv("WAREHOUSE_PASSWORD")

    if not password:
        raise ValueError("WAREHOUSE_PASSWORD is missing")

    with psycopg.connect(
        host=os.getenv("WAREHOUSE_HOST", "localhost"),
        port=int(os.getenv("WAREHOUSE_PORT", "5433")),
        dbname=os.getenv("WAREHOUSE_DB", "weather"),
        user=os.getenv("WAREHOUSE_USER", "weather"),
        password=password,
        connect_timeout=10,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(UPSERT_SQL, records)

    return len(records)