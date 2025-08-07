import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from tqdm import tqdm

FILE_PATH = './dbdata/yellow_tripdata_2025-01.parquet'
df = pd.read_parquet(FILE_PATH, engine="pyarrow").head(100)

print(df)

def read_parquet_chunk(file_path, chunk_size=100_000):
    pq_file = pq.ParquetFile(file_path)
    for batch in pq_file.iter_batches(batch_size=chunk_size):
        return batch.to_pandas()
    return pd.DataFrame()

# creating connection to postgres
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
# engine.connect()

def load_all_chunks():
    try:
        pq_file = pq.ParquetFile(FILE_PATH)

        first_batch = next(pq_file.iter_batches(batch_size=1))
        pd.DataFrame.from_records(first_batch.to_pydict()).head(0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace', index=False)

        total_amt = pq_file.metadata.num_rows
        with tqdm(total=total_amt, unit='row') as pbar:
            for batch in pq_file.iter_batches(batch_size=100_000):
                df = batch.to_pandas()
                df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append', index=False, chunksize=10000)
                pbar.update(len(df))
                pbar.refresh()

        print(f"Loaded {total_amt} rows in table yellow_taxi_data")

    except Exception as e:
        print(f"An error occured: {e}")
    finally:
        engine.dispose()

if __name__ == '__main__':
    load_all_chunks()

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

