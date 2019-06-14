import json
import logging
import traceback
from s3.s3_helper import write_to_s3
from kafka import KafkaConsumer

from config import configuration

config = configuration['local']

class KafkaConsumerService():
    """
    Initialises a kafka consumer with the specified Topic name and configurations
    """

    def __init__(self, consumer_config: dict, topic_name: str):
        if not consumer_config or len(consumer_config) == 0:
            logging.error("Error creating a consumer, no consumer config supplied")
            raise Exception("You must pass in a consumer config for Kafka consumer.")

        self.logger = logging.getLogger(__name__)
        try:
            self.consumer = KafkaConsumer(**consumer_config)
            self.consumer.subscribe([topic_name])

        except Exception as exc:
            raise ValueError("Error occured while constructing the consumer :{0}".format(exc))

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.consumer.close()
        if args[0]:
            traceback.print_exception(args[0], args[1], args[2])

    def consume(self):
        raw_messages = self.consumer.poll(10000)
        if len(raw_messages) > 0:
            for consumer_records in raw_messages.values():
                for consumer_record in consumer_records:
                    print(consumer_record.value)

                    try:
                        write_to_s3(consumer_record.value)
                    except Exception as exc:
                        self.logger.exception("Consumer service raised exception: [{exc} consumer record: "
                                              "[{record}]".format(exc=exc, record=consumer_record))
                    finally:
                        self.consumer.commit()
        self.consumer.commit()


def main():
    """
    The main method supplies the properties of the kafka_related consumer
    KAFKA_BOOTSTRAP_SERVER: The kafka_related broker URL with the right port number read from config file
    auto_offset_reset: a Kafka config on how to reset the kafka_related offsets.
    enable_auto_commit: Disabling Auto commits of the kafka_related offsets
    value_deserializer: We specify kafka_related to use JSON value_deserializer
    group_id: kafka_related consumer group_id
    max_poll_records : Max number of records to fetch in 1 poll
    timeout_ms: ms to wait if messages are not available in buffer

    """

    KAFKA_BOOTSTRAP_SERVER = config.bootstrap_server
    print(KAFKA_BOOTSTRAP_SERVER)
    consumer_config = dict(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
                           auto_offset_reset='earliest',
                           enable_auto_commit=False,
                           value_deserializer=lambda m: json.loads(m.decode('ascii')),
                           group_id=config.group_id,
                           max_poll_records=config.max_poll_records
                           )
    with KafkaConsumerService(consumer_config=consumer_config, topic_name=config.topic1) as consumer_service:
        consumer_service.consume()


if __name__ == '__main__':
    main()
