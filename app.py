import time
import random

from sqlalchemy import create_engine
# from sqlalchemy_utils import database_exists, create_database

from dotenv import load_dotenv
import os

load_dotenv(override=True)

db_settings = {
    "host": os.getenv('POSTGRES_HOST'),
    "port": int(os.getenv('POSTGRES_PORT')),
    "user": os.getenv('POSTGRES_USER'),
    "password": os.getenv('POSTGRES_PASSWORD'),
    "db": os.getenv('POSTGRES_DB'),
}

portal_db_settings = {
    "host": os.getenv('PORTAL_POSTGRES_HOST'),
    "port": int(os.getenv('PORTAL_POSTGRES_PORT')),
    "user": os.getenv('PORTAL_POSTGRES_USER'),
    "password": os.getenv('PORTAL_POSTGRES_PASSWORD'),
    "database": os.getenv('PORTAL_POSTGRES_DB'),
}

# connect to the database
db_string = 'postgresql://{}:{}@{}:{}/{}'.format(db_settings.get('user'), db_settings.get('password'), db_settings.get('host'), db_settings.get('port'), db_settings.get('db'))
db = create_engine(db_string)



# def validate_database():
#      engine = create_engine(db_string)
#      if not database_exists(engine.url): # Checks for the first time  
#          create_database(engine.url)     # Create new DB    
#          print("New Database Created"+database_exists(engine.url)) # Verifies if database is there or not.
#      else:
#          print("Database Already Exists")


from flask import Flask

server = Flask(__name__)

@server.route("/")
def hello():
  return "Hello World!"

if __name__ == "__main__":
  server.run(host='0.0.0.0',port=8006)
