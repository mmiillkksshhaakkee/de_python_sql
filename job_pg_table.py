import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from tqdm import tqdm
import os
import argparse
import gc
from datetime import datetime, timedelta


def read_parquet_chunk(file_path, chunk_size=100_000):
    pq_file = pq.ParquetFile(file_path)
    for batch in pq_file.iter_batches(batch_size=chunk_size):
        return batch.to_pandas()
    return pd.DataFrame()


def main_load(parameters):

    user = parameters.user
    password = parameters.password
    host = parameters.host
    port = parameters.port
    db = parameters.db
    table_name = parameters.table_name
    url = parameters.url

    datafile = "output.parquet"

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
        pq_file = pq.ParquetFile(datafile)

        first_batch = next(pq_file.iter_batches(batch_size=1))
        pd.DataFrame.from_records(first_batch.to_pydict()).head(0).to_sql(name=table_name, con=engine, if_exists='replace', index=False)

        total_amt = pq_file.metadata.num_rows
        with tqdm(total=total_amt, unit='row') as pbar:
            for batch in pq_file.iter_batches(batch_size=100_000):
                df = batch.to_pandas()
                df.to_sql(name=table_name, con=engine, if_exists='append', index=False, chunksize=10000)
                pbar.update(len(df))
                pbar.refresh()
                del df
                gc.collect()


        print(f"Loaded {total_amt} rows in table yellow_taxi_data")

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

    main_load(args)

#try:
#    chunk = read_parquet_chunk(FILE_PATH, chunk_size=100_000)
#
#    if not chunk.empty:
#        print(f"Read {len(chunk)} raws from Parquet file")
#
#        chunk.head(0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')
#        chunk.to_sql(name='yellow_taxi_data', con=engine, if_exists='append', index=False)
#
#        print(f"Data successfully read and loaded to PG from Parquet file")
#    else:
#        print(f"Parquet file is either empty or not readable")
#except Exception as e:
#    print(f"An error occured: {e}")
#finally:
#    engine.dispose()

# do not need pd.to_datetime() function because timestamp columns are already in the correct format
# print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine)) # <=== this creates postgresql command

#df = pd.read_parquet('./dbdata/yellow_tripdata_2025-01.parquet', engine="pyarrow")
#df.head(0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')

# for df_chunk in df:
#       print(df_chunk.shape)
#pqf = pq.ParquetFile('./dbdata/yellow_tripdata_2025-01.parquet')
#batch_ = pqf.iter_batches(batch_size=100_000)
#batch_.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')
#print(batch_.shape)

