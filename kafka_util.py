from kafka import KafkaProducer, KafkaConsumer
import json
from log_util import advanced_log

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