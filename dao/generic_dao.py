import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from config import configuration

config = configuration['local']


def create_db_engine():
    """
    :return: Returns an engine object that can be used to create session object
    """
    db_url = 'postgresql://{}:{}@{}:{}/{}'
    db_url = db_url.format(config.database_user, config.database_password, config.database_host,
                           config.database_port, config.database_db)
    engine = create_engine(db_url)
    return engine


def create_session(db_engine):
    """

    :param db_engine: Takes in a database engine object
    :return: Returns a session that can be used to query database
    """
    session = sessionmaker(bind=db_engine)
    return session


def read_data_from_db(sql_query: str, session):
    """

    :param sql_query: The query that needs to be executed to get data from database
    :param session: Session required to make the DB session
    :return:
    """
    try:
        data = session.execute(sql_query)
        return data
    except Exception as exc:
        logging.error("Error querying database:.." + exc)
    finally:
        session.commit()
