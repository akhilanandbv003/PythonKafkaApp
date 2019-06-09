from db_config import config
import os

environment = os.environ.get("ENV") or 'default'
con