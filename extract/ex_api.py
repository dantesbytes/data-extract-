from urllib import request
import certifi
import json
import sqlite3
import pandas as pd
import os
import urllib3

def source_data_from_parquet(parquet_file_name):
    try:
        df_parquet = pd.read_parquet(parquet_file_name)
        print(f"Successfully loaded data from {parquet_file_name}")
    except Exception as e:
        print(f"Error reading parquet file: {e}")
        df_parquet = pd.DataFrame()
    return df_parquet

def source_data_from_api(api_endpoint):
    try:
         
        http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())
        api_response = http.request('GET', api_endpoint)
        api_status = api_response.status
        if api_status == 200:
            data = json.loads(api_response.data.decode('utf-8'))
            df_api = pd.json_normalize(data)
            print(f"Successfully loaded data from API: {api_endpoint}")
        else:
            print(f"API request failed with status code: {api_status}")
            df_api = pd.DataFrame()
    except Exception as e:
        print(f"Error accessing API: {e}")
        df_api = pd.DataFrame()
    return df_api

def source_data_from_table(db_name, table_name):
    try:
        
        with sqlite3.connect(db_name) as conn:
            df_table = pd.read_sql(f"SELECT * from {table_name}", conn)
        print(f"Successfully loaded data from table: {table_name}")
    except Exception as e:
        print(f"Error reading from database: {e}")
        df_table = pd.DataFrame()
    return df_table

def extract_data(parquet_file_name=None, api_endpoint=None, db_name=None, table_name=None):
    results = {}
    
    if parquet_file_name:
        results['parquet'] = source_data_from_parquet(parquet_file_name)
    
    if api_endpoint:
        results['api'] = source_data_from_api(api_endpoint)
    
    if db_name and table_name:
        results['table'] = source_data_from_table(db_name, table_name)
    
    return results


if __name__ == "__main__":
    api_endpoint = "https://data.cityofnewyork.us/resource/h9gi-nx95.json?$limit=500"
    parquet_file = "path/to/your/parquet_file.parquet"  
    db_name = "your_database.db"  
    table_name = "your_table"  
    
    extracted_data = extract_data(
        parquet_file_name=parquet_file,
        api_endpoint=api_endpoint,
        db_name=db_name,
        table_name=table_name
    )
    
    for source, df in extracted_data.items():
        print(f"\nData from {source}:")
        print(df.head())
        print(f"Shape: {df.shape}")