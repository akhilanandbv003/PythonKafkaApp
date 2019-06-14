from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy import DDL
from sqlalchemy import event
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base

from config import configuration
from dao.generic_dao import create_db_engine

config = configuration['local']
Base = declarative_base()

event.listen(Base.metadata, 'before_create', DDL("CREATE SCHEMA IF NOT EXISTS {}".format(config.database_schema)))
meta = MetaData(schema=config.database_schema)

"""
Run this DB migration script to recreate the schema and tables needed
"""

class Factory(Base):
    __tablename__ = 'latex_factory'
    factory_id = Column(Integer, primary_key=True)
    factory_name = Column(String)
    factory_description = Column(String)
    factory_built_date = Column(DateTime)


class Product(Base):
    __tablename__ = 'latex_product'
    product_id = Column(Integer, primary_key=True)
    product_name = Column(String)
    inventor = Column(String)


class Machine(Base):
    __tablename__ = 'latex_machine'
    machine_id = Column(Integer, primary_key=True)
    machine_name = Column(String)
    machine_creation_timestamp = Column(DateTime)
    product_id = Column(Integer, ForeignKey("latex_product.product_id"))


class Item(Base):
    __tablename__ = 'latex_item'
    item_id = Column(Integer, primary_key=True)
    machine_id = Column(Integer, ForeignKey("latex_machine.machine_id"))
    factory_id = Column(Integer, ForeignKey("latex_factory.factory_id"))
    product_id = Column(Integer, ForeignKey("latex_product.product_id"))
    item_manufactured_timestamp = Column(Integer)


def main():
    engine = create_db_engine()
    Base.metadata.create_all(engine)


if __name__ == '__main__':
    main()
