import json
import pandas as pd
import fastavro

WORK_DIR = '/rowdata'
def ingest_json(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    df = pd.DataFrame(data)
    return df

# Example usage
df_ad_impressions = ingest_json(f'{WORK_DIR}/ad_impressions.json')



def ingest_avro(file_path):
    with open(file_path, 'rb') as file:
        reader = fastavro.reader(file)
        records = [record for record in reader]
    df = pd.DataFrame(records)
    return df

# Example usage
df_bid_requests = ingest_avro(f'{WORK_DIR}/bid_requests.avro')


def ingest_csv(file_path):
    df = pd.read_csv(file_path)
    return df

# Example usage
df_clicks_conversions = ingest_csv(f'{WORK_DIR}/clicks_conversions.csv')


def validate_data(df, required_columns):
    for column in required_columns:
        if column not in df.columns:
            raise ValueError(f'Missing required column: {column}')
    df = df.drop_duplicates()
    return df

# Example usage
required_columns = ['user_id', 'timestamp']
df_ad_impressions = validate_data(df_ad_impressions, required_columns)
df_clicks_conversions = validate_data(df_clicks_conversions, required_columns)
df_bid_requests = validate_data(df_bid_requests, required_columns)

def correlate_data(impressions_df, clicks_df):
    merged_df = pd.merge(impressions_df, clicks_df, on='user_id', suffixes=('_impression', '_click'))
    return merged_df

# Example usage
df_correlated = correlate_data(df_ad_impressions, df_clicks_conversions)


#### Data storage 
from sqlalchemy import create_engine

engine = create_engine('postgresql://username:password@localhost/advertisex')

def store_data(df, table_name):
    df.to_sql(table_name, engine, if_exists='replace', index=False)

# Example usage
store_data(df_correlated, 'correlated_data')


## 
def safe_ingest(func, *args):
    try:
        return func(*args)
    except Exception as e:
        print(f"Error: {e}")
        return None

# Example usage
df_ad_impressions = safe_ingest(ingest_json, f'{WORK_DIR}/ad_impressions.json')

from flask import Flask, jsonify

app = Flask(__name__)

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy'})

if __name__ == '__main__':
    app.run(debug=True)

################
import os

def main():
    json_path = f'{WORK_DIR}/ad_impressions.json'
    csv_path = f'{WORK_DIR}/clicks_conversions.csv'
    avro_path = f'{WORK_DIR}/bid_requests.avro'

    df_ad_impressions = safe_ingest(ingest_json, json_path)
    df_clicks_conversions = safe_ingest(ingest_csv, csv_path)
    df_bid_requests = safe_ingest(ingest_avro, avro_path)

    if df_ad_impressions is not None and df_clicks_conversions is not None:
        df_correlated = correlate_data(df_ad_impressions, df_clicks_conversions)
        store_data(df_correlated, 'correlated_data')
    if df_ad_impressions is not None and df_bid_requests is not None:
        df_correlated_bid = correlate_data(df_ad_impressions, df_bid_requests)
        store_data(df_correlated_bid, 'correlated_data')

if __name__ == '__main__':
    main()
