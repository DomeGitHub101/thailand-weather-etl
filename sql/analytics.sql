SELECT
    city_code,
    (observed_at AT TIME ZONE 'Asia/Bangkok')::date AS weather_date,
    COUNT(*) AS hourly_records,
    ROUND(AVG(temperature_c)::numeric, 2) AS avg_temperature_c,
    MAX(temperature_c) AS max_temperature_c,
    MIN(temperature_c) AS min_temperature_c,
    ROUND(SUM(precipitation_mm)::numeric, 2) AS total_precipitation_mm
FROM weather_hourly
GROUP BY
    city_code,
    (observed_at AT TIME ZONE 'Asia/Bangkok')::date
ORDER BY city_code, weather_date;