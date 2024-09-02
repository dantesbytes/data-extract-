from urllib import request
import certifi
import json
import sqlite3
import pandas as pd
import os

def source_data_from_parquet(parquet_file_name):
    try:
        df_parquet = pd.read_parquet(parquet_file_name)
    except Exception as e:
        print(f"Error reading parquet file: {e}")
        df_parquet = pd.DataFrame()
    return df_parquet

def source_data_from_csv(csv_file_name):
    
    csv_file_path = os.path.join("C:", "Users", "dante", "development", "etl", "data_extract", "data", "h9gi-nx95.csv")
    try:
        df_csv = pd.read_csv(csv_file_path)
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        df_csv = pd.DataFrame()
    return df_csv


if __name__ == "__main__":
    
    parquet_df = source_data_from_parquet("your_parquet_file.parquet")
    print("Parquet DataFrame:", parquet_df.head())

    
    csv_df = source_data_from_csv("h9gi-nx95.csv")
    print("CSV DataFrame:", csv_df.head())