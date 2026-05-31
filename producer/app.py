import os
import time
import json
import logging
import signal
import sys
from collections import deque
from typing import Any, Dict
import requests
from confluent_kafka import Producer
from dotenv import load_dotenv

# Load local environment if available
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("github-producer")

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "github-events")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "10"))

# Globals for graceful shutdown
running = True

def sigterm_handler(signum: int, frame: Any) -> None:
    """Handle termination signals."""
    global running
    logger.info("Termination signal received. Shutting down gracefully...")
    running = False

signal.signal(signal.SIGINT, sigterm_handler)
signal.signal(signal.SIGTERM, sigterm_handler)

def kafka_delivery_report(err: Any, msg: Any) -> None:
    """Callback for Kafka delivery reports."""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def fetch_github_events(headers: Dict[str, str]) -> list:
    """Poll GitHub public events API."""
    url = "https://api.github.com/events"
    try:
        response = requests.get(url, headers=headers, timeout=10)
        
        # Log rate limit info in debug or info if low
        remaining = response.headers.get("X-RateLimit-Remaining")
        reset_time = response.headers.get("X-RateLimit-Reset")
        
        if remaining:
            logger.debug(f"GitHub API Rate Limit Remaining: {remaining}")
            if int(remaining) < 10:
                logger.warning(f"GitHub API Rate Limit is very low: {remaining} left.")
                
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 403:
            logger.error("GitHub API Rate limit exceeded (403 Forbidden).")
            if reset_time:
                sleep_duration = max(0, int(reset_time) - int(time.time())) + 2
                logger.info(f"Sleeping for {sleep_duration} seconds until rate limit resets...")
                time.sleep(min(sleep_duration, 120))  # sleep max 2 mins at a time in loop
            return []
        else:
            logger.error(f"Failed to fetch GitHub events: HTTP {response.status_code} - {response.text}")
            return []
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error while fetching GitHub events: {e}")
        return []

def main() -> None:
    """Main Producer Loop."""
    logger.info("Initializing Real-Time GitHub Event Producer...")
    logger.info(f"Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Kafka Topic: {KAFKA_TOPIC}")
    logger.info(f"Polling Interval: {POLL_INTERVAL_SECONDS} seconds")

    # Set up Kafka Producer
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "github-producer",
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 500,
    }
    
    try:
        producer = Producer(conf)
    except Exception as e:
        logger.critical(f"Failed to create Kafka producer: {e}")
        sys.exit(1)

    # Set up GitHub API Headers
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "GitHub-RealTime-Event-Pipeline",
    }
    if GITHUB_TOKEN:
        logger.info("GitHub PAT token provided, authenticated requests will be made (5,000 req/hr).")
        headers["Authorization"] = f"token {GITHUB_TOKEN}"
    else:
        logger.warning("No GITHUB_TOKEN env variable found. Using unauthenticated requests (60 req/hr).")

    # Bounded list of processed event IDs to avoid sending duplicates to Kafka
    processed_event_ids = deque(maxlen=1000)
    seen_set = set()

    logger.info("Starting polling loop...")
    while running:
        start_time = time.time()
        
        events = fetch_github_events(headers)
        new_events_count = 0
        
        for event in reversed(events):  # Process oldest first to maintain sequential order in Kafka
            event_id = event.get("id")
            if not event_id:
                continue
                
            if event_id not in seen_set:
                # Add to deduplication structures
                seen_set.add(event_id)
                processed_event_ids.append(event_id)
                # Keep seen_set synchronized with deque
                if len(seen_set) > 1000:
                    # Remove oldest elements
                    while len(seen_set) > len(processed_event_ids):
                        # Simple cleanup of set
                        seen_set = set(processed_event_ids)
                        break

                # Prepare payload
                # We extract essential fields and keep raw fields as needed
                payload = {
                    "id": event_id,
                    "type": event.get("type"),
                    "repo_id": event.get("repo", {}).get("id"),
                    "repo_name": event.get("repo", {}).get("name"),
                    "actor_login": event.get("actor", {}).get("login"),
                    "created_at": event.get("created_at"),
                }
                
                try:
                    # Serialize and publish
                    serialized_payload = json.dumps(payload).encode("utf-8")
                    producer.produce(
                        KAFKA_TOPIC, 
                        key=event_id.encode("utf-8"), 
                        value=serialized_payload, 
                        callback=kafka_delivery_report
                    )
                    new_events_count += 1
                except BufferError:
                    logger.warning("Kafka local queue full, flushing and waiting...")
                    producer.poll(0.5)
                    # Retry once
                    try:
                        producer.produce(KAFKA_TOPIC, key=event_id.encode("utf-8"), value=serialized_payload, callback=kafka_delivery_report)
                        new_events_count += 1
                    except Exception as retry_err:
                        logger.error(f"Failed to produce message on retry: {retry_err}")
                except Exception as produce_err:
                    logger.error(f"Error producing message to Kafka: {produce_err}")

        # Flush Kafka buffer to deliver messages
        producer.poll(0)
        
        if new_events_count > 0:
            logger.info(f"Published {new_events_count} new GitHub events to Kafka.")
            
        # Calculate polling sleep
        elapsed = time.time() - start_time
        sleep_needed = max(0.1, POLL_INTERVAL_SECONDS - elapsed)
        
        # Poll Kafka events for callbacks periodically
        time.sleep(sleep_needed)

    # Clean up before exit
    logger.info("Flushing remaining Kafka messages...")
    producer.flush(timeout=5.0)
    logger.info("Producer stopped.")

if __name__ == "__main__":
    main()
