import json
import logging
import traceback

from confluent_kafka import Consumer
from confluent_kafka import KafkaException
from kafka import KafkaConsumer

from config import configuration


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

            self.create_confluent_consumer(topic_name)
        except Exception as exc:
            raise ValueError("Error occured while constructing the consumer :{0}".format(exc))

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.consumer.close()
        if args[0]:
            traceback.print_exception(args[0], args[1], args[2])

    def create_confluent_consumer(self, topic_name):
        global record_count, batch_size, poll_count
        try:
            consumer_config = self.create_consumer_config(self)
            kafka_consumer = Consumer(consumer_config)
            kafka_consumer.subscribe([topic_name])
            return kafka_consumer
        except Exception as ex:
            raise ValueError("Error occured while constructing the consumer :{0}".format(ex))

    def consume_messages(self, max_poll_count):
        """

        :param max_poll_count: Maximum number of polls to run before stopping the consumer
        batch_size : Number of records before closing the poll
        The while loop keeps polling for records until the record_count reaches the batch_size and
        poll_count is less than max_poll_count
        This logic can be tweaked as per our project requirement.
        """
        global record_count , poll_count

        # It is used only here hence not moving to app.conf for better visibilty.
        batch_size = 100

        try:
            self.consumer = self.create_confluent_consumer()
            while record_count < batch_size and poll_count < max_poll_count :
                try:
                    message = self.consumer.poll()
                    logging.debug("Entering Kafka consumer poll cycle...")

                    if message is None:
                        logging.info("Poll returned no messages")
                        poll_count += 1
                        continue
                    if message.error():
                        poll_count += 1
                        raise KafkaException(message.error())

                    else:
                        logging.info('Received message: {}'.format(message.offset()))
                        record_count += 1
                        poll_count = 0

                        data = message.value().decode('utf-8')
                        print(data)
                except Exception as exc:
                    logging.info('Exception {} occured while reading messages from topic '.format(exc))

        except Exception as exc:
            logging.info('Exception occured while polling data : {}'.format(exc))

    @staticmethod
    def create_consumer_config(self):
        config = configuration['local']
        x = {
            "api.version.request": True,
            "enable.auto.commit": True,
            "group.id": config.group_id,
            "bootstrap.servers": config.bootstrap_server,
            "default.topic.config": {
                "auto.offset.reset": config.auto_offset_reset
            }
        }
        return x

    def consume(self):
        raw_messages = self.consumer.poll(10000)

        # for consumer_records in raw_messages.values():
        #     for consumer_record in consumer_records:
        #         print(consumer_record.value)

        for topic_partition, messages in raw_messages.items():
            print(topic_partition)
            print(messages)
            print(messages.topicPartition)
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
    with KafkaConsumerService(consumer_config=consumer_config, topic_name=config.topic1) as consumer_service:
        # consumer_service.consume()
        consumer_service.consume_messages(topic_name=config.topic1)


if __name__ == '__main__':
    main()
