CREATE TABLE IF NOT EXISTS weather_hourly (
    city_code TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    temperature_c DOUBLE PRECISION NOT NULL,
    relative_humidity_pct DOUBLE PRECISION NOT NULL,
    precipitation_mm DOUBLE PRECISION NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (city_code, observed_at),

    CHECK (relative_humidity_pct BETWEEN 0 AND 100),
    CHECK (precipitation_mm >= 0)
);