import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from tqdm import tqdm
import os
import argparse
import gc

url_csv="https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
datafile = "zones.csv"

def load_csv(parameters):

    user = parameters.user
    password = parameters.password
    host = parameters.host
    port = parameters.port
    db = parameters.db
    table_name = parameters.table_name
    url = parameters.url

    #FILE_PATH = './dbdata/yellow_tripdata_2025-01.parquet'
    #df = pd.read_parquet(FILE_PATH, engine="pyarrow").head(100)

    if not os.path.exists(datafile):
        print(f"File {datafile} not found, downloading...")
        ret = os.system(f"wget {url} -O {datafile}")
        if ret != 0:
            print(f"Failed to download {datafile}")
            return
    else:
        print(f"File {datafile} exists, skipping...")

    # creating connection to postgres
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    # engine.connect()
    try:
       df_zones = pd.read_csv(datafile)
       df_zones.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
    except Exception as e:
        print(f"An error occured: {e}")
    finally:
        engine.dispose()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='ingest parquet data into a database')
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='db name for postgres')
    parser.add_argument('--table_name', help='table name for where the result will be stored')
    parser.add_argument('--url', help='url of the file')

    args = parser.parse_args()

    load_csv(args)
