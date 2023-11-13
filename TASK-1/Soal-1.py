import pandas as pd

# membaca file CSV
df = pd.read_csv(f'../dataset/yellow_tripdata_2020-07.csv', sep=",", low_memory=False)

# menampilkan data frame
print(df.dtypes)