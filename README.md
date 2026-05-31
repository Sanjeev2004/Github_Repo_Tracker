# Real-Time GitHub Event Processing Pipeline

An MVP pipeline for processing public GitHub events with Python, Kafka, Spark
Structured Streaming, PostgreSQL, Docker, and Power BI.

## Project Layout

- `producer/`: GitHub API polling and Kafka publishing
- `streaming/`: Spark Structured Streaming transforms
- `database/`: PostgreSQL schema
- `dashboards/`: Power BI notes and screenshots
- `docker-compose.yml`: local infrastructure
