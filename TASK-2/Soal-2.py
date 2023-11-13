import pandas as pd

def get_file_path():
    path = ('../dataset/yellow_tripdata_2023-01.parquet')
    return path

def get_dataframe(path):
    df = pd.read_parquet(path, engine='fastparquet')
    return df


path = get_file_path()
df = get_dataframe(path)
print(df)