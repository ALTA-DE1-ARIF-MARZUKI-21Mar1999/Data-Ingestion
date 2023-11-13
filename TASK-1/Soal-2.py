import pandas as pd

# membaca file CSV
df = pd.read_csv(f'../dataset/yellow_tripdata_2020-07.csv', sep=",", low_memory=False)

# menampilkan data frame
print(df.dtypes)
print('--------------------')

rename_column = df.rename(columns={'VendorID' : 'vendor_id', 'RatecodeID' : 'Rate_code_id', 'PULocationID' : 'PU_location_id', 'DOLocationID' : 'DO_location_id', })
print(rename_column.dtypes)