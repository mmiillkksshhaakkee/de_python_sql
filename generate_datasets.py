import pandas as pd

import random
from datetime import datetime, timedelta
from pyarrow import table
from sqlalchemy import create_engine, text
import argparse
from faker import Faker
from faker_commerce import Provider as CommerceProvider
from tqdm import tqdm
import gc

#TODO: add more complex generating of products' names
# add truncating table if exists

# initializing
fake = Faker()
fake.add_provider(CommerceProvider)

DB_CONFIG = {
    'user': 'root',
    'password': 'root',
    'host': 'localhost',
    'port': '5432',
    'db': 'analytics_dashboard',
    'schema': 'app_schema',
    'table_name' : 'users products transactions user_actions'
}

n_rows_users = 10_000
n_rows_products = 1000
n_rows_transactions = 100_000
n_rows_user_actions = 200_000

def generate_user_batch(start_id, batch_size, existing_ids=None):
    """Generator of fake data using batch upload to Postgres DB"""
    """Filtering only unique emails"""
    emails_set = set()
    emails_list = []

    while len(emails_set) < batch_size:
        emails_set.add(fake.unique.email())

    emails_list = list(emails_set)

    return {
        'user_id': list(range(start_id, start_id + batch_size)),
        'first_name': [fake.first_name() for _ in range(batch_size)],
        'last_name': [fake.last_name() for _ in range(batch_size)],
        'email': [fake.unique.email() for _ in range(batch_size)],
        'created_dttm': [fake.date_time_this_year(before_now=True) for _ in range(batch_size)]
    }
def generate_product_batch(start_id, batch_size, existing_ids=None):
    return {
        'product_id': list(range(start_id, start_id + batch_size)),
        'product_name': [fake.unique.ecommerce_name() for _ in range(batch_size)],
        'price': [round(random.uniform(500.0, 50000.0), 2) for _ in range(batch_size)],
        'created_dttm':[fake.date_time_this_decade(before_now=True) for _ in range(batch_size)]
    }

def generate_transaction_batch(start_id, batch_size, existing_id):
    return {
        'transaction_id': list(range(start_id, start_id + batch_size)),
        'user_id': [fake.random.choice(existing_id['users']) for _ in range(batch_size)],
        'product_id': [fake.random.choice(existing_id['products']) for _ in range(batch_size)],
        'amount': [round(fake.random.uniform(10.0, 1000.0), 2) for _ in range(batch_size)],
        'transaction_status': [fake.random.choice(["pending", "completed", "cancelled", "shipped"]) for _ in range(batch_size)],
        'created_dttm': [fake.date_time_this_year(before_now=True) for _ in range(batch_size)]
    }

def generate_user_action_batch(start_id, batch_size, existing_id):
    return {
        'action_id': list(range(start_id, start_id + batch_size)),
        'user_id': [fake.random.choice(existing_id['users']) for _ in range(batch_size)],
        'action_type': [fake.random.choice([
            'product_click',
            'product_search',
            'search_query',
            'filter_products',
            'sort_products',
            'add_to_wishlist',
            'remove_from_wishlist',
            'product_share',
            'add_to_cart',
            'remove_from_cart',
            'update_cart_quantity',
            'view_cart',
            'cart_abandoned',
            'checkout_start',
            'payment_method_selected',
            'payment_attempt',
            'payment_success',
            'payment_failed',
            'order_confirmed'
        ]) for _ in range(batch_size)],
        'created_dttm': [fake.date_time_this_year(before_now=True) for _ in range(batch_size)]
    }

def get_existing_ids(engine, tbl_name, id_column):
    try:
        with engine.connect() as conn:
            query = text(f"select {id_column} from {tbl_name}")
            result = conn.execute(query)
            return [row[0] for row in result]
    except Exception as e:
        print(f"Error getting ID from {tbl_name}: {e}")
        return []

def batch_upload_table(engine, schema, tbl_name, rows_num, batch_size=10_000, existing_ids=None):

    function_map = {
        'users' : generate_user_batch,
        'products' : generate_product_batch,
        'transactions' : generate_transaction_batch,
        'user_actions' : generate_user_action_batch
    }

    # Deciding if we need foreign keys for the table
    need_existing_ids = tbl_name in ['transactions', 'user_actions']

    with tqdm(total=rows_num, desc=f"Uploading into {tbl_name}", unit='row') as pbar:
        for i in range(0, rows_num, batch_size):
            current_batch_size = min(batch_size, rows_num - i)

            # Generating data...
            if need_existing_ids and existing_ids:
                data_batch = function_map[tbl_name](i, current_batch_size, existing_ids)
            else:
                data_batch = function_map[tbl_name](i, current_batch_size)

            df_batch = pd.DataFrame(data_batch)
            df_batch.to_sql(
                name=tbl_name,
                con=engine,
                schema=schema,
                if_exists='append',
                index=False,
                method='multi'
            )

            pbar.update(current_batch_size)

            # Memory drop
            del data_batch, df_batch
            gc.collect()

def main_gen_load(batch_size):

    """user = parameters.user
    password = parameters.password
    host = parameters.host
    port = parameters.port
    db = parameters.db
    table_name = [x.strip() for x in parameters.table_name.split()] if parameters.table_name else []
    url = parameters.url"""

    engine = create_engine(f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['db']}")
    schema = DB_CONFIG['schema']

    with engine.connect() as conn:
        conn.execute(text(f"set search_path to {schema}"))
        conn.commit()

    # Default table order in case we get an empty string
    table_name = [x.strip() for x in DB_CONFIG['table_name'].split()]
    load_order = ['users', 'products', 'transactions', 'user_actions']

    # Filtering order if we have sent them in parameter
    load_order = [t for t in load_order if t in table_name]

    # Existing IDs dictionary
    existing_ids = {}

    rows_map = {
        'users' : n_rows_users,
        'products' : n_rows_products,
        'transactions' : n_rows_transactions,
        'user_actions' : n_rows_user_actions
    }

    for t in load_order:
        print(f"Loading {t}...")

        if t in ['transactions', 'user_actions']:
            if 'users' not in existing_ids:
                existing_ids['users'] = get_existing_ids(engine, 'users', 'user_id')
            if t == 'transactions' and 'products' not in existing_ids:
                existing_ids['products'] = get_existing_ids(engine, 'products', 'product_id')

        batch_upload_table(
            engine=engine,
            schema=schema,
            tbl_name=t,
            rows_num=rows_map[t],
            batch_size=batch_size,
            existing_ids=existing_ids if t in ['transactions', 'user_actions'] else None
        )

    print(f"\nAll data loaded successfully!")
    engine.dispose()

if __name__ == '__main__':
    """parser = argparse.ArgumentParser(description='ingest parquet data into a database')
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='db name for postgres')
    parser.add_argument('--table_name', help='table name for where the result will be stored')
    parser.add_argument('--url', help='url of the file')
    args = parser.parse_args()"""

    main_gen_load(10_000)

