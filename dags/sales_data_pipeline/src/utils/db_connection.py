import os
import psycopg2 as psy
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

def validate_env_variables():
    required_vars = [
        "DB_USER",
        "DB_PASSWORD",
        "DB_HOST",
        "DB_PORT",
        "DB_NAME"
    ]

    for var in required_vars:
        if not os.getenv(var):
            raise ValueError(f"{var} is not set in environment variables")

def get_connection():
    validate_env_variables()
    connection = psy.connect(database= os.getenv("DB_NAME"), 
                            user = os.getenv("DB_USER"), 
                            host = os.getenv("DB_HOST"), 
                            password = os.getenv("DB_PASSWORD"), 
                            port = os.getenv("DB_PORT"))

    return connection


def get_engine():
    validate_env_variables()
    connection_string = f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    return create_engine(connection_string)

