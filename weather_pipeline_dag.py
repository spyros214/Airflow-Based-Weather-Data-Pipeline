from __future__ import annotations
from datetime import datetime, timedelta
from typing import Dict, Any, List

import time
import pandas as pd
import requests

from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.models.param import Param
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException

DEFAULT_CITY = "Athens"
DEFAULT_LAT = 37.9838
DEFAULT_LON = 23.7275

RAW_DATASET = Dataset("postgresql://warehouse_db/weather/raw_hourly")
FACT_DATASET = Dataset("postgresql://warehouse_db/weather/fact_daily_weather")

with DAG(
    dag_id="weather_api_to_postgres_daily",
    start_date=datetime(2025, 1, 1),
    schedule="0 2 * * *",  # daily 02:00
    catchup=False,
    default_args={
        "owner": "data-eng",
        "retries": 2, 
        "retry_delay": timedelta(minutes=5),
    },
    params={
        "city": Param(DEFAULT_CITY, type="string"),
        "lat": Param(DEFAULT_LAT, type="number"),
        "lon": Param(DEFAULT_LON, type="number"),
    },
    tags=["weather", "api", "postgres", "daily"],
):

    @task(retries=2)  
    def extract(params: Dict[str, Any]) -> List[Dict[str, Any]]:
        lat = params["lat"]; lon = params["lon"]; city = params["city"]

        url = (
            "https://api.open-meteo.com/v1/forecast"
            f"?latitude={lat}&longitude={lon}"
            "&hourly=temperature_2m,windspeed_10m"
            "&timezone=UTC&past_days=2"
        )

        last_err = None
        for attempt in range(3):
            try:
                r = requests.get(url, timeout=15)
                r.raise_for_status()
                data = r.json() or {}
                hourly = data.get("hourly") or {}
                times = hourly.get("time") or []
                temps = hourly.get("temperature_2m") or []
                winds = hourly.get("windspeed_10m") or []

                if not times or not temps:
                    raise AirflowSkipException("Open-Meteo returned no hourly data")

                rows: List[Dict[str, Any]] = []
                for t, temp, wind in zip(times, temps, winds or [None] * len(times)):
                    rows.append({
                        "city": city,
                        "latitude": data.get("latitude"),
                        "longitude": data.get("longitude"),
                        "timezone": data.get("timezone", "UTC"),
                        "obs_time": pd.to_datetime(t, utc=True).isoformat(),
                        "temperature_c": temp,
                        "windspeed_kmh": wind,
                        "source": "open-meteo",
                    })
                return rows

            except (requests.Timeout, requests.ConnectionError) as e:
                last_err = e
                time.sleep(2 * (attempt + 1))  # backoff
            except AirflowSkipException:
                raise
            except Exception as e:
                raise

        raise RuntimeError(f"Failed to fetch Open-Meteo after retries: {last_err}")

    @task
    def transform(rows: List[Dict[str, Any]]) -> pd.DataFrame:
        df = pd.DataFrame(rows)
        df.dropna(subset=["obs_time", "temperature_c"], inplace=True)
        df["temperature_c"] = pd.to_numeric(df["temperature_c"], errors="coerce")
        df["windspeed_kmh"] = pd.to_numeric(df["windspeed_kmh"], errors="coerce")
        df["obs_time"] = pd.to_datetime(df["obs_time"], utc=True)
        df["ingestion_ts"] = pd.Timestamp.utcnow()
        return df

    @task
    def validate(df: pd.DataFrame) -> pd.DataFrame:
        # φίλτραρε αντί για assert ώστε να μην αποτυγχάνει το task
        df = df[df["temperature_c"].between(-80, 70)]
        df = df[df["windspeed_kmh"].between(0, 250)]
        if df.empty:
            raise AirflowSkipException("No sane rows to load")
        return df

    @task(outlets=[RAW_DATASET])
    def load_raw(df: pd.DataFrame) -> int:
        hook = PostgresHook(postgres_conn_id="postgres_warehouse")
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
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
            """)
            # απλό insert row-by-row για αρχή
            for _, r in df.iterrows():
                cur.execute("""
                    INSERT INTO weather.raw_hourly
                    (city, latitude, longitude, timezone, obs_time, temperature_c, windspeed_kmh, ingestion_ts, source)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT DO NOTHING
                """, (
                    r["city"], r.get("latitude"), r.get("longitude"), r.get("timezone"),
                    pd.to_datetime(r["obs_time"]).to_pydatetime().replace(tzinfo=None),
                    float(r["temperature_c"]) if pd.notna(r["temperature_c"]) else None,
                    float(r["windspeed_kmh"]) if pd.notna(r["windspeed_kmh"]) else None,
                    pd.Timestamp.utcnow().to_pydatetime().replace(tzinfo=None),
                    r.get("source", "open-meteo"),
                ))
        return len(df)

    @task(outlets=[FACT_DATASET])
    def build_fact(_rowcount: int, city: str) -> int:
        hook = PostgresHook(postgres_conn_id="postgres_warehouse")
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                INSERT INTO weather.fact_daily_weather
                (city, date, temp_avg_c, temp_min_c, temp_max_c, windspeed_avg_kmh, record_count)
                SELECT
                  city,
                  DATE(obs_time) AS date,
                  AVG(temperature_c) AS temp_avg_c,
                  MIN(temperature_c) AS temp_min_c,
                  MAX(temperature_c) AS temp_max_c,
                  AVG(windspeed_kmh) AS windspeed_avg_kmh,
                  COUNT(*) AS record_count
                FROM weather.raw_hourly
                WHERE city = %s
                GROUP BY city, DATE(obs_time)
                ON CONFLICT (city, date) DO UPDATE SET
                  temp_avg_c = EXCLUDED.temp_avg_c,
                  temp_min_c = EXCLUDED.temp_min_c,
                  temp_max_c = EXCLUDED.temp_max_c,
                  windspeed_avg_kmh = EXCLUDED.windspeed_avg_kmh,
                  record_count = EXCLUDED.record_count,
                  ingestion_ts = NOW();
            """, (city,))
            cur.execute("SELECT COUNT(*) FROM weather.fact_daily_weather WHERE city = %s", (city,))
            return cur.fetchone()[0]

    @task
    def notify(n: int) -> None:
        print(f"Fact rows (all dates for city): {n}")

    # --- wiring ---
    extracted = extract(params={"city": DEFAULT_CITY, "lat": DEFAULT_LAT, "lon": DEFAULT_LON})
    transformed = transform(extracted)
    validated = validate(transformed)
    inserted = load_raw(validated)
    affected = build_fact(inserted, city=DEFAULT_CITY)
    notify(affected)
