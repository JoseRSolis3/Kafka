from kafka import KafkaProducer, KafkaConsumer
import json
from log_util import advanced_log
class_name = lambda var: var.__class__.__name__


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

    # Usage Example → Producer.initialize(port, "topic_name", {"key": "value"})
    def initialize(self, port):
        hostServer = lambda port: f"localhost:{port}"
        if port is None:
            advanced_log("warning",f"Port is None, returning None. Please try again.")
            return None
        if isinstance(port, str):
            try:
                int(port)
                port = hostServer(port)
                self.producer = KafkaProducer(
                    bootstrap_servers=[port],
                    value_serializer = lambda v: json.dumps(v).encode("utf-8")
                )
            except:
                advanced_log("warning",f"Invalid port. please try again.")
                return None

        elif isinstance(port, int):                            
            port = hostServer(str(port))
            self.producer = KafkaProducer(
                bootstrap_servers=[port],
                value_serializer = lambda v: json.dumps(v).encode("utf-8")
            )
        else:
            advanced_log("warning",f"Invalid data type, returning None. Please try again.")
            return None
        return self
    
    def sendMessage(self, topic, values):
        variables = [topic, values]
        for var in variables:
            if var is None:
                advanced_log("warning",f"{var} is None, returning None. Please try again.")
                return self
        
        if self.producer is None:
            advanced_log("warning",f"Producer not initiated. Initiate: Producer.initialize(port).")
            return self
        
        if isinstance(topic, str):
            advanced_log("info",f"Confirmed, topic data type is: {class_name(topic)}.")
        else:
            advanced_log("warning",f"Invalid data type. Please try again.")
            return None

        if isinstance(values, dict):
            advanced_log("info",f"Confirmed, values data type is: {class_name(values)}")
        else:
            advanced_log("warning",f"Invalid data type. Please try again.")
            return None
        
        self.producer.send(topic, values)
        self.producer.flush()
        return self
            
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