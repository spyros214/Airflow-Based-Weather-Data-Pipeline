CREATE SCHEMA IF NOT EXISTS weather; 

CREATE TABLE IF NOT EXISTS weather.raw_hourly (
  city TEXT NOT NULL,
  latitude DOUBLE PRECISION,
  longitude DOUBLE PRECISION,
  timezone TEXT,
  obs_time TIMESTAMP NOT NULL,
  temperature_c DOUBLE PRECISION,
  windspeed_kmh DOUBLE PRECISION,
  ingestion_ts TIMESTAMP NOT NULL DEFAULT NOW(),
  source TEXT NOT NULL DEFAULT 'open-meteo'
);

CREATE TABLE IF NOT EXISTS weather.fact_daily_weather (
  city TEXT NOT NULL,
  date DATE NOT NULL,
  temp_avg_c DOUBLE PRECISION,
  temp_min_c DOUBLE PRECISION,
  temp_max_c DOUBLE PRECISION,
  windspeed_avg_kmh DOUBLE PRECISION,
  record_count INT,
  ingestion_ts TIMESTAMP NOT NULL DEFAULT NOW(),
  PRIMARY KEY (city, date)
);
