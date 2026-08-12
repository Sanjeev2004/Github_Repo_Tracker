FROM python:3.10-slim-bookworm

# Prevent python from writing pyc files and buffering stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install Default JRE (Headless), build-essential, and postgres client libraries
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre-headless \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Set Java Home to default symlink managed by Debian
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$PATH:$JAVA_HOME/bin

WORKDIR /app

# Install requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-fetch the Spark Kafka packages during build to cache them.
# This avoids delay or download failures at container startup.
RUN python -c "from pyspark.sql import SparkSession; SparkSession.builder.appName('cache').config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0').getOrCreate().stop()"

# Copy source code
COPY spark_job.py .

RUN useradd --create-home --uid 10001 appuser && chown -R appuser:appuser /app
USER appuser

CMD ["python", "spark_job.py"]
