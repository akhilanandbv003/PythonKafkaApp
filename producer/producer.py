import logging
import json
from kafka import KafkaProducer
from dao.generic_dao import *

SQL_QUERY = """ SELECT * FROM latex_item 
                LEFT JOIN  latex_product ON latex_item.product_id = latex_product.product_id
                LEFT JOIN latex_machine ON latex_item.machine_id = latex_machine.machine_id
                LEFT JOIN latex_factory ON latex_item.factory_id = latex_factory.factory_id"""

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
            logging.info("Producing to the topic"+topic_name)
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
        session = create_session(create_db_engine())
        records = dict(read_data_from_db(session))
        print(records)
        for y in records:
            x = json.dumps(dict(y))
            publisher.produce_to_kafka(x)


if __name__ == '__main__':
    main()