import pandas as pd
from sqlalchemy import create_engine

postgres_conn_str = 'postgresql://postgres:pass@localhost:5432/store'
engine_postgres = create_engine(postgres_conn_str)

order_df = pd.read_sql('SELECT * FROM "orders"', con=engine_postgres)
order_details_df = pd.read_sql('SELECT * FROM order_details', con=engine_postgres)
brands_df = pd.read_sql('SELECT * FROM brands', con=engine_postgres)
products_df = pd.read_sql('SELECT * FROM products', con=engine_postgres)

citus_conn_str = 'postgresql://postgres:pass@localhost:15432/store'
engine_citus = create_engine(citus_conn_str)

order_df.to_sql('orders', con=engine_citus, index=False, if_exists='replace', method='multi')
order_details_df.to_sql('order_details', con=engine_citus, index=False, if_exists='replace', method='multi')
brands_df.to_sql('brands', con=engine_citus, index=False, if_exists='replace', method='multi')
products_df.to_sql('products', con=engine_citus, index=False, if_exists='replace', method='multi')


