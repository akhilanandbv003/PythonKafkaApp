from config import configuration

config = configuration['local']
db_url = 'postgresql://{}:{}@{}:{}/{}'
db_url = db_url.format(config.database_user, config.database_password, config.database_host,
                           config.database_port, config.database_db)
