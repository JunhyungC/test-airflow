import redis
import json
import datetime

# --- Configuration (Match these with your Airflow DAG and Redis setup) ---
# Assuming Redis is running on the host machine and accessible via localhost
# based on the 'host.docker.internal' usage in docker-compose for containers
# to reach the host.
REDIS_HOST = 'localhost'
REDIS_PORT = 6399  # Port from AIRFLOW_CONN_REDIS_STREAM
REDIS_DB = 0
STREAM_NAME = 'data_event_stream' # Stream name from redis_stream_dag.py
# --- End Configuration ---

def send_message_to_stream(message_data):
    """Connects to Redis and sends a message to the specified stream."""
    try:
        # Connect to Redis
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
        r.ping() # Check connection
        print(f"Successfully connected to Redis at {REDIS_HOST}:{REDIS_PORT}")

        # Add message to the stream using XADD
        # The '*' tells Redis to generate an ID automatically.
        # The message_data dictionary is sent as key-value pairs.
        message_id = r.xadd(STREAM_NAME, message_data)

        print(f"Successfully sent message to stream '{STREAM_NAME}' with ID: {message_id.decode()}")

    except redis.exceptions.ConnectionError as e:
        print(f"Error connecting to Redis: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    # Example message payload (customize as needed)
    timestamp = datetime.datetime.now().isoformat()
    payload = {
        "event_time": timestamp,
        "source": "manual_script",
        "data": json.dumps({"key1": "value1", "key2": 123}),
        "status": "new"
    }

    print(f"Attempting to send message to stream '{STREAM_NAME}': {payload}")
    send_message_to_stream(payload)