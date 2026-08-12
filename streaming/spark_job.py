import os
import sys
import logging
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType, LongType

# Load local environment if available
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("spark-streaming-job")

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "github-events")
POSTGRES_DB = os.getenv("POSTGRES_DB", "github_events")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
CHECKPOINT_DIR = os.getenv("SPARK_CHECKPOINT_DIR", "/checkpoints/github-events")
POSTGRES_CONNECT_TIMEOUT_SECONDS = int(os.getenv("POSTGRES_CONNECT_TIMEOUT_SECONDS", "10"))

SPARK_WINDOW_DURATION = os.getenv("SPARK_WINDOW_DURATION", "5 minutes")
SPARK_SLIDE_DURATION = os.getenv("SPARK_SLIDE_DURATION", "1 minute")
SPARK_WATERMARK_DURATION = os.getenv("SPARK_WATERMARK_DURATION", "10 minutes")

def write_to_postgres(df, epoch_id) -> None:
    """
    ForeachBatch writer that executes database upserts per partition.
    This runs in parallel across Spark executor nodes.
    """
    logger.info(f"Processing micro-batch epoch {epoch_id}...")
    
    # We define the inner function that will execute on each executor partition
    def partition_upsert(partition) -> None:
        import psycopg2
        
        # Connect to Postgres database from the executor
        conn = psycopg2.connect(
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            connect_timeout=POSTGRES_CONNECT_TIMEOUT_SECONDS
        )
        cursor = conn.cursor()
        
        upsert_query = """
            INSERT INTO repository_activity (window_start, window_end, repository_name, event_type, event_count)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (window_start, window_end, repository_name, event_type)
            DO UPDATE SET event_count = EXCLUDED.event_count;
        """
        
        rows_written = 0
        try:
            for row in partition:
                cursor.execute(
                    upsert_query,
                    (row.window_start, row.window_end, row.repository_name, row.event_type, row.event_count)
                )
                rows_written += 1
            conn.commit()
            if rows_written > 0:
                print(f"Partition upserted {rows_written} aggregated event window records successfully.")
        except Exception as e:
            conn.rollback()
            logger.exception("Error writing partition batch to PostgreSQL")
            raise
        finally:
            cursor.close()
            conn.close()

    # Trigger partition-level upsert execution
    df.foreachPartition(partition_upsert)

def main() -> None:
    """Spark Streaming Entry Point."""
    logger.info("Initializing Spark Structured Streaming Job...")
    logger.info(f"Reading from Kafka topic '{KAFKA_TOPIC}' on broker '{KAFKA_BOOTSTRAP_SERVERS}'")
    logger.info(f"Target DB: {POSTGRES_DB} at {POSTGRES_HOST}:{POSTGRES_PORT}")
    logger.info(f"Aggregating windows: {SPARK_WINDOW_DURATION} sliding every {SPARK_SLIDE_DURATION}")

    # Build SparkSession with Kafka integration packages
    # Note: org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 is standard for Spark 3.4.x
    spark = SparkSession.builder \
        .appName("GitHubEventStreamingPipeline") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    # Define strict schema for incoming JSON payloads from Kafka
    github_event_schema = StructType([
        StructField("id", StringType(), True),
        StructField("type", StringType(), True),
        StructField("repo_id", LongType(), True),
        StructField("repo_name", StringType(), True),
        StructField("actor_login", StringType(), True),
        StructField("created_at", StringType(), True)
    ])

    # Read binary stream from Kafka
    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", os.getenv("KAFKA_STARTING_OFFSETS", "latest")) \
        .option("failOnDataLoss", os.getenv("KAFKA_FAIL_ON_DATA_LOSS", "true").lower() == "true") \
        .load()

    # Convert binary values to String, parse JSON schema, extract fields, and parse ISO timestamp
    parsed_events = kafka_stream \
        .selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json("json_str", github_event_schema).alias("data")) \
        .select("data.*") \
        .withColumn("created_time", to_timestamp(col("created_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))

    # Apply watermark to handle late/out-of-order data
    watermarked_events = parsed_events \
        .withWatermark("created_time", SPARK_WATERMARK_DURATION)

    # Perform sliding window aggregation
    # Groups by sliding window, repository name, and event type
    aggregated_metrics = watermarked_events \
        .groupBy(
            window(col("created_time"), SPARK_WINDOW_DURATION, SPARK_SLIDE_DURATION),
            col("repo_name"),
            col("type")
        ) \
        .count() \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("repo_name").alias("repository_name"),
            col("type").alias("event_type"),
            col("count").alias("event_count")
        )

    # Write aggregated streams into PostgreSQL using foreachBatch and checkpointing
    # Checkpointing is crucial for Structured Streaming fault tolerance
    checkpoint_dir = CHECKPOINT_DIR
    
    query = aggregated_metrics.writeStream \
        .foreachBatch(write_to_postgres) \
        .outputMode("update") \
        .option("checkpointLocation", checkpoint_dir) \
        .start()

    logger.info("Spark structured stream active. Listening for events...")
    query.awaitTermination()

if __name__ == "__main__":
    main()
