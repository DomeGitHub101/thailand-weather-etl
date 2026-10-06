import requests


def extract_weather(latitude, longitude, start_date, end_date):
    url = "https://archive-api.open-meteo.com/v1/archive"

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": (
            "temperature_2m,"
            "relative_humidity_2m,"
            "precipitation"
        ),
        "models": "era5",
        "timezone": "Asia/Bangkok",
    }

    response = requests.get(
        url,
        params=params,
        timeout=30,
    )
    response.raise_for_status()

    return response.json()