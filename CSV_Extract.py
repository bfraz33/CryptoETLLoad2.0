
#Importing Libraries
import requests
import csv
import pandas as pd
import logging
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, MetaData
import os
import yaml

def load_config(file_path):
    with open('config.yaml', 'r') as config_file:
        config = yaml.safe_load(config_file)

    return config


def get_crypto_data(api_key):
    url = f"https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest?CMC_PRO_API_KEY={api_key}"
    headers = {
        'Accept': 'application/json',
        'Content-type': 'application/json'
    }
    response = requests.get(url, headers=headers)
    return response.json()

def prepare_crypto_data(data):
    crypto_data = []
    csvheader = ['id', 'name', 'symbol', 'circulating_supply', 'total_supply', 'price', 'percent_change_1h', 'percent_change_24h', 'percent_change_7d', 'percent_change_30d', 'percent_change_60d', 'percent_change_90d',
             'market_cap', 'market_cap_dominance', 'fully_diluted_market_cap' 'last_updated']

    for x in data['data']:
            listing = [
                x['id'], x['name'], x['symbol'],
                x['circulating_supply'], x['total_supply'],
                x['quote']['USD'].get('price', None),  # Handle missing key
                x['quote']['USD'].get('percent_change_1h', None),  # Handle missing key
                x['quote']['USD'].get('percent_change_24h', None),  # Handle missing key
                x['quote']['USD'].get('percent_change_30d', None),  # Handle missing key
                x['quote']['USD'].get('percent_change_60d', None),  # Handle missing key
                x['quote']['USD'].get('percent_change_90d', None),  # Handle missing key
                x['quote']['USD'].get('market_cap', None),  # Handle missing key
            ]
            crypto_data.append(listing)  # Append each listing to the list

    return csvheader, crypto_data  # Return the entire crypto_data list


def write_to_csv(file_path, header, data):
    with open('CMCcrypto.csv', 'w', encoding='utf8', newline='') as f:
      writer = csv.writer(f)
      writer.writerow(header)
      writer.writerows(data)

def configure_logging(log_file_path):
    script_directory = os.path.dirname(os.path.abspath(__file__))
    log_file_path = os.path.join(script_directory, 'etl_log.log')
    logging.basicConfig(
      filename=log_file_path,
      level=logging.INFO,
      format='%(asctime)s - ${levelname)s - %(message)s'
)

#Path to CSV file in local drive
csv_file_path = r'C:\ETLDATA\Crypto ETL Project'

def create_database_schema(engine):
  metadata = MetaData()
  crypto_data = Table(
    'crypto_data', metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String),
    Column('symbol', String),
    Column('circulating_supply', Float),
    Column('total_supply', Float),
    Column('price', Float),
    Column('percent_change_1h', Float),
    Column('percent_change_24h', Float),
    Column('percent_change_7d', Float),
    Column('percent_change_30d', Float),
    Column('percent_change_60d', Float),
    Column('percent_change_90d', Float),
    Column('market_cap', Float),
    Column('market_cap_dominance', Float),
    Column('fully_diluted_market_cap', Float),
    Column('last_updated', String)  # Change the data type if needed
)

def load_data_to_database(engine,csv_file_path):
    try:
       data = pd.read_csv('C:\ETLDATA\Crypto ETL Project\CMCcrypto.csv')
       data.to_sql('Crypto_data', engine, if_exists='replace', index=False)
       logging.info("ETL process completed.")
    except Exception as e:
       print("an exception has occurred:", e)

if __name__ == "__main__":
    config = load_config('config.yaml')
    api_key = config['api_key']
    csv_file_path = config['csv_file_path']
    log_file_path = config['log_file_path']
    db_config = config['db_config']

    response = get_crypto_data(api_key)
    header, crypto_data = prepare_crypto_data(response)
    write_to_csv(csv_file_path, header, crypto_data)
    configure_logging(log_file_path)
    engine = create_engine(
        f'postgresql://{db_config["user"]}:{db_config["password"]}@{db_config["host"]}:{db_config["port"]}/{db_config["database"]}'
    )
    create_database_schema(engine)
    load_data_to_database(engine, csv_file_path)

# Wait for user input before exiting
#input("Press enter to exit..."