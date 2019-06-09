"""
Parses app-conf.cnf for app configs and provides config variables
"""
import configparser

class LocalConfig:
    config = configparser.ConfigParser()
    config.read("/Users/akhilanandbv/PycharmProjects/PythonKafkaApp/app-conf.cnf")

    bucket_name = config.get('s3', 'bucket_name')
    bootstrap_server = config.get('kafka', 'bootstrap_server')
    group_id = config.get('kafka', 'group_id')
    topic1 = config.get('kafka', 'topic1')
    max_poll_records = config.getint('kafka', 'max_poll_records')
    batch_size = config.getint('kafka', 'batch_size')
    auto_offset_reset = config.get('kafka', 'auto_offset_reset')



configuration ={
    'local' :LocalConfig
}