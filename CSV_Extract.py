import requests
import csv
import pandas as pd
import psycopg2
import logging
from sqlalchemy import create_engine
import os



from requests.api import head

url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest?CMC_PRO_API_KEY=Your_api_key"

#Defining data
headers = {
    'Accept': 'application/json',
    'Content-type': 'application/json'
}

#Get request
response = requests.request("GET", url, headers=headers,data={})


#Pasrsing Json Response
myJson = response.json()

#Prepping data for CSV
ourData =[]
csvheader = ['id', 'name', 'symbol', 'max_supply', 'circulating_supply', 'total_supply', 'infinite_supply', 'platform', 'cmc_rank', 'last_updated',
             'volume_24h', 'volume_change_24h', 'percent_change_1h', 'percent_change_24h', 'percent_change_7d', 'percent_change_30d', 'percent_change_60d', 'percent_change_90d',
             'market_cap', 'market_cap_dominance', 'fully_diluted_market_cap' 'last_updated']

for x in myJson['data']:
    # Use .get() method to handle missing keys gracefully
    listing = [
        x['id'], x['name'], x['symbol'], x.get('max_supply', None),
        x['circulating_supply'], x['total_supply'], x.get('platform', None),
        x['last_updated'],
        x['quote']['USD'].get('volume_24h', None),  # Handle missing key
        x['quote']['USD'].get('volume_change_24h', None),  # Handle missing key
        x['quote']['USD'].get('percent_change_1h', None),  # Handle missing key
        x['quote']['USD'].get('percent_change_24h', None),  # Handle missing key
        x['quote']['USD'].get('percent_change_7d', None),  # Handle missing key
        x['quote']['USD'].get('percent_change_30d', None),  # Handle missing key
        x['quote']['USD'].get('percent_change_60d', None),  # Handle missing key
        x['quote']['USD'].get('percent_change_90d', None),  # Handle missing key
        x['quote']['USD'].get('market_cap', None),  # Handle missing key
        x['quote']['USD'].get('market_cap_dominance', None),  # Handle missing key
        x['quote']['USD'].get('fully_diluted_market_cap', None)  # Handle missing key
    ]

    ourData.append(listing)

#Write data to CSV
with open('CMCcrypto.csv', 'w', encoding='utf8', newline='') as f:
    writer = csv.writer(f)

    writer.writerow(csvheader)
    writer.writerows(ourData)

print(myJson)
input("Press enter to continue...")

#Calculating the script directory
script_directory = os.path.dirname(os.path.abspath(__file__))

#Setup up logging
log_file_path = os.path.join(script_directory, 'etl_log.log')
logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    format='%(asctime)s - ${levelname)s - %(message)s'
)

#Setting my PostgreSQL connection
db_config = {
    'host': 'localhost',
    'port': 5432,
    'user': 'postgres',
    'password': 'pwd',
    'database': 'Crypto',
}
#Path to CSV file in local drive
csv_file_path = r'C:\ETLDATA\Crypto ETL Project\CMCcrypto.csv'

try:
 data = pd.read_csv('C:\ETLDATA\Crypto ETL Project\CMCcrypto.csv')
#Creating connection to the postgresql database
 conn = psycopg2.connect(
    host=db_config['host'],
    port=db_config['port'],
    user=db_config['user'],
    password=db_config['password'],
    database=db_config['database']

 )
except Exception as e:
    print("an exception has occurred:", e)


engine = create_engine(
    f'postgresql://{db_config["user"]}:{db_config["password"]}@{db_config["host"]}:{db_config["port"]}/{db_config["database"]}'
)
try:
    # Loading data into the PostgreSQL table
    data.to_sql('Crypto_data', engine, if_exists='replace', index=False)

    conn.close()

    # Log the successful completion
    logging.info("ETL process completed.")
except Exception as e:

    # Log the error if something goes wrong
    logging.error(f"Error during ETL process: {e}")

# Wait for user input before exiting
input("Press enter to exit...")
