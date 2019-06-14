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
    auto_offset_reset = config.get('kafka', 'auto_offset_reset')

    dbname = config.get('database', 'dbname')
    database_host = config.get('database', 'host')
    database_user = config.get('database', 'user')
    database_password = config.get('database', 'password')
    database_db = config.get('database', 'db')
    database_port = config.get('database', 'port')
    database_schema = config.get('database', 'schema')



configuration ={
    'local' :LocalConfig
}