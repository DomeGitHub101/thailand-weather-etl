from datetime import datetime, timedelta, timezone


# เวลาไทย UTC+7 สำหรับข้อมูลปัจจุบันที่โปรเจกต์นี้ใช้
THAILAND_TZ = timezone(timedelta(hours=7))


def transform_weather(payload, city_code):
    hourly = payload["hourly"]

    times = hourly["time"]
    temperatures = hourly["temperature_2m"]
    humidities = hourly["relative_humidity_2m"]
    precipitation = hourly["precipitation"]

    lengths = {
        len(times),
        len(temperatures),
        len(humidities),
        len(precipitation),
    }

    if len(lengths) != 1:
        raise ValueError("Hourly arrays have different lengths")

    if not times:
        raise ValueError("The API returned no hourly data")

    records = []
    seen_times = set()

    for time_text, temperature, humidity, rain in zip(
        times,
        temperatures,
        humidities,
        precipitation,
        strict=True,
    ):
        if any(
            value is None
            for value in (time_text, temperature, humidity, rain)
        ):
            raise ValueError(f"Missing weather value at {time_text}")

        observed_at = datetime.fromisoformat(time_text)

        # API ที่ขอด้วย Asia/Bangkok อาจส่งเวลาโดยไม่มี offset
        if observed_at.tzinfo is None:
            observed_at = observed_at.replace(tzinfo=THAILAND_TZ)
        else:
            observed_at = observed_at.astimezone(THAILAND_TZ)

        if observed_at in seen_times:
            raise ValueError(f"Duplicate timestamp: {time_text}")

        seen_times.add(observed_at)

        temperature = float(temperature)
        humidity = float(humidity)
        rain = float(rain)

        if not 0 <= humidity <= 100:
            raise ValueError(f"Invalid humidity at {time_text}")

        if rain < 0:
            raise ValueError(f"Negative precipitation at {time_text}")

        records.append(
            {
                "city_code": city_code,
                "observed_at": observed_at,
                "temperature_c": temperature,
                "relative_humidity_pct": humidity,
                "precipitation_mm": rain,
            }
        )

    return records