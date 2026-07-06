import time
import json
import random
from datetime import datetime, timezone
from google.cloud import pubsub_v1

# ---- CONFIG ----
PROJECT_ID = "streamsight-project"
TOPIC_ID = "streamsight-events"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

EVENT_TYPES = ["click", "page_view", "purchase", "signup", "logout"]

def generate_fake_event():
    return {
        "event_id": random.randint(100000, 999999),
        "event_type": random.choice(EVENT_TYPES),
        "user_id": f"user_{random.randint(1, 500)}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "latency_ms": round(random.uniform(20, 400), 2)
    }

def publish_event():
    event = generate_fake_event()
    data = json.dumps(event).encode("utf-8")
    future = publisher.publish(topic_path, data)
    print(f"Published: {event}  | Message ID: {future.result()}")

if __name__ == "__main__":
    print(f"Publishing fake events to {topic_path} ... (Ctrl+C to stop)")
    try:
        while True:
            publish_event()
            time.sleep(1)  # one event per second
    except KeyboardInterrupt:
        print("\nStopped publishing.")