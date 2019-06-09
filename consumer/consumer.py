import json
import logging
import traceback
from s3.s3_helper import write_to_s3
from kafka import KafkaConsumer

from config import configuration


class KafkaConsumerService():
    """
    Initialises a kafka consumer with the specified Topic name and configurations
    """

    def __init__(self, consumer_config: dict, topic_name:str):
        if not consumer_config or len(consumer_config) == 0:
            logging.error("Error creating a consumer, no consumer config supplied")
            raise Exception("You must pass in a consumer config for Kafka consumer.")
        self.logger = logging.getLogger(__name__)
        group_id = consumer_config['group_id']
        print(group_id)
        try:
            # self.consumer = KafkaConsumer(topic_name, group_id= group_id,auto_offset_reset='earliest',value_deserializer=lambda m: json.loads(m.decode('ascii')))
            self.consumer = KafkaConsumer(topic_name, **consumer_config)
            # self.consumer.subscribe([topic_name])
        except Exception as exc:
            raise ValueError("Error occured while constructing the consumer :{0}".format(exc))

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.consumer.close()
        if args[0]:
            traceback.print_exception(args[0], args[1], args[2])

    def consume(self):
        raw_messages = self.consumer.poll(3000)
        print(raw_messages)
        if len(raw_messages) > 0:
            for _, consumer_records in raw_messages.items():
                for consumer_record in consumer_records:
                    # try:
                        # write_to_s3()
                    print(consumer_record.value)
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
    config = configuration['local']
    KAFKA_BOOTSTRAP_SERVER = config.bootstrap_server
    print(KAFKA_BOOTSTRAP_SERVER)
    consumer_config = dict(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
                           auto_offset_reset='earliest',
                           enable_auto_commit=False,
                           value_deserializer=lambda m: json.loads(m.decode('ascii')),
                           group_id=config.group_id,
                           max_poll_records=config.max_poll_records
                           )
    with KafkaConsumerService(consumer_config = consumer_config , topic_name=config.topic1) as consumer_service:
        consumer_service.consume()


if __name__ == '__main__':
    main()
