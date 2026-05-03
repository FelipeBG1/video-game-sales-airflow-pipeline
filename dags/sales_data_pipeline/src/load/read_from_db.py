import pandas as pd
from src.utils.db_connection import get_engine

def read_from_db(table):

    engine = get_engine()
    try:     
        df = pd.read_sql(f"SELECT * FROM {table}", con=engine)
        return df
    finally:
        engine.dispose()