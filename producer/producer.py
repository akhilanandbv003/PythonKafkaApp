import logging
import json
from kafka import KafkaProducer


class MessagePublisher:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self

    def produce_to_kafka(self, topic_name: str, message: str):
        config = {
            'api_version': (0, 10),
            'bootstrap_servers': "localhost:9092",
            'value_serializer': lambda v: json.dumps(v).encode('utf-8')
        }
        producer = KafkaProducer(**config)

        try:
            producer.send(topic=topic_name, value=message)
        except Exception:
            self.logger.error(
                "Encountered error connecting / sending message for message: [{message}]".format(message=message))
            raise Exception
        finally:
            producer.flush()
            producer.close()





def main():
    with MessagePublisher() as publisher:
        publisher.produce_to_kafka("test01", "HELLO KAFKA")


if __name__ == '__main__':
    main()
