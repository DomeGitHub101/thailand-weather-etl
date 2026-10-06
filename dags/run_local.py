import json
from pathlib import Path

from dotenv import load_dotenv

from weather_etl.extract import extract_weather
from weather_etl.transform import transform_weather
from weather_etl.load import load_weather


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def main():
    # utf-8-sig รองรับไฟล์ UTF-8 ที่มี BOM จาก Windows PowerShell
    load_dotenv(
        PROJECT_ROOT / ".env",
        encoding="utf-8-sig",
    )

    city_code = "bangkok"
    target_date = "2025-01-01"

    print(f"Extracting {city_code} weather for {target_date}...")

    payload = extract_weather(
        latitude=13.7563,
        longitude=100.5018,
        start_date=target_date,
        end_date=target_date,
    )

    # เก็บข้อมูลต้นฉบับไว้ตรวจสอบ
    data_directory = PROJECT_ROOT / "data"
    data_directory.mkdir(parents=True, exist_ok=True)

    raw_file = data_directory / f"{city_code}_{target_date}.json"
    raw_file.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"Saved raw data: {raw_file.name}")

    records = transform_weather(payload, city_code)

    print(f"Transformed {len(records)} records")

    # เวอร์ชันนี้ขอข้อมูลหนึ่งวันของกรุงเทพฯ
    if len(records) != 24:
        raise ValueError(
            f"Expected 24 hourly records, received {len(records)}"
        )

    processed_count = load_weather(records)

    print(f"Loaded or updated {processed_count} records")
    print("ETL completed successfully")


if __name__ == "__main__":
    main()