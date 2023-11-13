import pandas as pd

# membaca file CSV
df = pd.read_csv(f'../dataset/yellow_tripdata_2020-07.csv', sep=",", low_memory=False)

# menampilkan data frame
# print(df.dtypes)
# print('--------------------')

df = df.rename(columns={'VendorID' : 'vendor_id', 'RatecodeID' : 'Rate_code_id', 'PULocationID' : 'PU_location_id', 'DOLocationID' : 'DO_location_id', })
print(df.dtypes)
print('--------------------')

top_10_passanger_count = df.nlargest(10, 'passenger_count')[['vendor_id', 'passenger_count', 'trip_distance', 'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge']]
# print(top_10_passanger_count)

df['passenger_count'].fillna(0, inplace=True)
df['passenger_count'] = df['passenger_count'].astype(int)
print(df.dtypes)