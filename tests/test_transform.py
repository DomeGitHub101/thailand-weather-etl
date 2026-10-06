from datetime import datetime, timedelta, timezone

import pytest

from weather_etl.transform import transform_weather


@pytest.fixture
def sample_payload():
    return {
        "hourly": {
            "time": [
                "2025-01-01T00:00",
                "2025-01-01T01:00",
            ],
            "temperature_2m": [25.5, 24.0],
            "relative_humidity_2m": [70, 75],
            "precipitation": [0.0, 1.2],
        }
    }


def test_transform_preserves_weather_values(sample_payload):
    records = transform_weather(sample_payload, "bangkok")

    assert len(records) == 2

    assert records[0]["city_code"] == "bangkok"
    assert records[0]["temperature_c"] == 25.5
    assert records[0]["relative_humidity_pct"] == 70.0
    assert records[0]["precipitation_mm"] == 0.0

    assert records[1]["temperature_c"] == 24.0
    assert records[1]["relative_humidity_pct"] == 75.0
    assert records[1]["precipitation_mm"] == 1.2


def test_transform_interprets_time_as_bangkok(sample_payload):
    records = transform_weather(sample_payload, "bangkok")
    observed_at = records[0]["observed_at"]

    assert observed_at.utcoffset() == timedelta(hours=7)

    # เที่ยงคืนวันที่ 1 เวลาไทย = 17:00 วันที่ 31 ใน UTC
    assert observed_at.astimezone(timezone.utc) == datetime(
        2024, 12, 31, 17, 0, tzinfo=timezone.utc
    )


def test_transform_rejects_different_array_lengths(sample_payload):
    sample_payload["hourly"]["temperature_2m"].pop()

    with pytest.raises(ValueError, match="different lengths"):
        transform_weather(sample_payload, "bangkok")


@pytest.mark.parametrize(
    "field",
    [
        "temperature_2m",
        "relative_humidity_2m",
        "precipitation",
    ],
)
def test_transform_rejects_missing_values(sample_payload, field):
    sample_payload["hourly"][field][0] = None

    with pytest.raises(ValueError, match="Missing weather value"):
        transform_weather(sample_payload, "bangkok")


@pytest.mark.parametrize("humidity", [-1, 101])
def test_transform_rejects_invalid_humidity(sample_payload, humidity):
    sample_payload["hourly"]["relative_humidity_2m"][0] = humidity

    with pytest.raises(ValueError, match="Invalid humidity"):
        transform_weather(sample_payload, "bangkok")


def test_transform_rejects_negative_rain(sample_payload):
    sample_payload["hourly"]["precipitation"][0] = -0.1

    with pytest.raises(ValueError, match="Negative precipitation"):
        transform_weather(sample_payload, "bangkok")


def test_transform_rejects_duplicate_times(sample_payload):
    sample_payload["hourly"]["time"][1] = "2025-01-01T00:00"

    with pytest.raises(ValueError, match="Duplicate timestamp"):
        transform_weather(sample_payload, "bangkok")


def test_transform_rejects_empty_data(sample_payload):
    for field in sample_payload["hourly"]:
        sample_payload["hourly"][field] = []

    with pytest.raises(ValueError, match="no hourly data"):
        transform_weather(sample_payload, "bangkok")