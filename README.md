# Airflow-Based Weather Data Pipeline
A fully automated **data engineering project** that ingests weather data from a public API, cleans and validates it, and stores it in a **PostgreSQL data warehouse**, orchestrated by **Apache Airflow**.

## Overview
This project demonstrates a real-world **ETL pipeline** built using modern data engineering tools.  
It fetches hourly weather data (temperature and wind speed) from the **Open-Meteo API**, processes it daily, and stores:
- Raw data in a staging table
- Aggregated metrics (daily averages/min/max) in a fact table

---

## Tech Stack
| Layer | Tool | Purpose |
|--------|------|----------|
| Orchestration | **Apache Airflow 2.x** | Schedule, monitor, and manage pipeline |
| Storage | **PostgreSQL** | Data warehouse |
| Data Processing | **Python (Pandas, Requests)** | Extract & Transform |
| Validation | **Assertions / Great Expectations** | Data quality checks |
| Containerization | **Docker Compose** | Local setup for Airflow + Postgres |

---

## Architecture
```
API (Open-Meteo)
      ↓
Airflow DAG (Extract → Transform → Validate → Load)
      ↓
PostgreSQL Warehouse
      ├─ weather.raw_hourly
      └─ weather.fact_daily_weather
```

## Features
- Automated ETL process orchestrated by Apache Airflow
- Modular task structure (extract, transform, load)
- Data validation and schema enforcement using SQL
- Extensible design for additional data sources or transformations
- Reproducible environment with Docker Compose

## Prerequisites:
- Docker and Docker Compose installed
- Python 
- Weather API key 
