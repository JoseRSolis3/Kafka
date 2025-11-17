from kafka import KafkaProducer, KafkaConsumer
import json
from log_util import advanced_log

# ==============================
# TODO: Producer Integration
# ==============================

# 1. Initialize your Kafka producer somewhere in your framework
#    - Consider creating a helper function like `producer_builder(host, topic, tvalue)`
#    - Ensure host is a string and valid
#    - Optionally wrap in try/except for connection errors

# 2. Sending messages
#    - Use `producer.send(topic, value)` for all message pushes
#    - Ensure the value is a dictionary or JSON-serializable object
#    - Consider emitting a log or GUI update on successful send
#    - If using UI buttons, connect their click signal to a producer send wrapper

# 3. Optional: Handle delivery results
#    - Kafka `send()` returns a Future
#    - Can use `.get(timeout=...)` to confirm delivery if needed
#    - Consider handling exceptions here

# ==============================
# TODO: Consumer Integration
# ==============================

# 1. Create a consumer worker class
#    - Should inherit QObject
#    - Include a pyqtSignal to emit messages (`message_received`)
#    - Store a `_running` boolean flag to control loop execution
#    - Implement `run()` method to poll messages and emit via signal
#    - Implement `stop()` method to set `_running = False` and break loop

# 2. Wrap the worker in a QThread
#    - Move worker to thread: `worker.moveToThread(thread)`
#    - Connect signals:
#       - `thread.started -> worker.run`
#       - `worker.message_received -> your GUI handler`
#       - `worker.finished -> thread.quit` and cleanup
#    - Start thread: `thread.start()`

# 3. GUI Integration
#    - Connect buttons or actions to `worker.start()` and `worker.stop()`
#    - Handle messages via a slot connected to `message_received`
#    - Ensure GUI updates happen in the main thread (signals take care of this)

# 4. Clean shutdown
#    - On app close, call `worker.stop()`
#    - Quit and wait for thread: `thread.quit()` and `thread.wait()`
#    - Ensure proper cleanup to avoid orphan threads

# ==============================
# TODO: Advanced / Optional
# ==============================

# 1. Pass kwargs into worker or plugin for Kafka configs
#    - e.g., `group_id`, `auto_offset_reset`, `enable_auto_commit`
#    - Keep defaults for simple use

# 2. Logging
#    - Use your `advanced_log` for all warnings/info
#    - Log producer sends and consumer receives for debugging

# 3. Error Handling
#    - Wrap Kafka operations in try/except to catch:
#       - `NoBrokersAvailable`
#       - `KafkaTimeoutError`
#       - JSON decode errors
#    - Optionally emit GUI warnings

# 4. Multi-topic support (if needed)
#    - Consider creating multiple consumer plugins
#    - Each can run in its own thread, emitting signals independently

# 5. Optional: Batch consumption
#    - If messages are high-volume, collect in batches and emit
#    - Avoid GUI overload with too many updates

# ==============================
# TODO: Testing & Verification
# ==============================

# 1. Test producer sends messages correctly
# 2. Test consumer receives messages and updates GUI
# 3. Test start/stop behavior dynamically via buttons
# 4. Test app shutdown to ensure threads close cleanly
# 5. Test invalid host/topic inputs to see log warnings

class Server:
    @staticmethod
    def ping(producer):
        try:
            if producer.bootstrap_connected():
                advanced_log("info", "Kafka producer is reachable.")
                return True
        except Exception as e:
            advanced_log("warning", f"No answer from Kafka: {e}")
            return False

class Producer():
    hostServer = lambda port: f"localhost:{port}"

    @staticmethod
    # Usage Example → Producer.setup(Producer.hostServer(9092), "topic_name", {"key": "value"})
    def setup(host, topic, tvalue):
        vars = (host, topic, tvalue)
        for var in vars:
            if var is None:
                advanced_log("warning",f"{var} is None, returning None. Please try again.")
                return None

        if isinstance(host, str):
            advanced_log("info",f"Confirmed! Returning host location.")
            producer = KafkaProducer(
                bootstrap_servers=[host],
                value_serializer = lambda v: json.dumps(v).encode("utf-8")
            )
        else:
            advanced_log("warning",f"Invalid data type, returning None. Please try again.")
            return None
        
        if isinstance(topic, str):
            advanced_log("info",f"Confirmed! Returning topic name.")
            topic = topic.strip().lower()
        else:
            advanced_log("warning","Invalid data type, returning None. Please try again.")
            return None

        if isinstance(tvalue, dict):
            advanced_log("info",f"Confirmed! Returning topic values")
            return producer.send(topic, (tvalue))
        else:
            advanced_log("warning",f"Invalid data type, returning None. Please try again.")
            return None
        
class Consumer():
    # Can be used as: Consumer.setup("my_topic", Producer.hostServer(9092))
    @staticmethod
    def setup(topic, host):
        variables = (topic , host)
        for var in variables:
            if var is None:
                return None
        if not isinstance(topic, str) or not isinstance(host, str):
            advanced_log("warning",f"Invalid data type, returning None. Please try again.")
            return None
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers = [host],
            value_deserializer = lambda x: json.loads(x.decode('utf-8'))
        )
        return consumer