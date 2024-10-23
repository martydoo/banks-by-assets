"""
Compile a list of the top ten largest banks by total assets.
Transform and store data in multiple currencies.
Save processed information in .csv and .db formats.
"""

import sqlite3
from datetime import datetime
import requests
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup

BANK_URL = 'https://en.wikipedia.org/wiki/List_of_largest_banks'
EXCHANGE_RATES = 'exchange_rate.csv'
INITIAL_ATTRIBUTES = ['Name', 'TA_USD_Billion']
FINAL_ATTRIBUTES = [
    'Name', 'TA_USD_Billion', 'TA_GBP_Billion',
    'TA_EUR_Billion', 'TA_INR_Billion'
    ]
OUTPUT_CSV = 'Largest_banks_data.csv'
DATABASE = 'Banks.db'
TABLE = "Largest_banks"
LOG_FILE = 'code_log.txt'


def log_progress(message):
    """Log each step of ETL pipeline."""
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)

    with open(LOG_FILE, 'a', encoding='UTF-8') as f:
        f.write(timestamp + ' : ' + message + '\n')


def extract(url, attributes):
    """Extract bank total assets data from Wikipedia link."""
    df = pd.DataFrame(columns=attributes)
    contents = requests.get(url, timeout=10).text
    soup = BeautifulSoup(contents, 'html.parser')

    body = soup.find_all('tbody')[0]
    rows = body.find_all('tr')

    count = 0
    for row in rows:
        if count >= 10:
            break
        cell_data = row.find_all('td')
        if not cell_data:
            continue

        bank = cell_data[1].text.replace('\n', '').strip()
        assets = cell_data[2].text.replace('\n', '')
        if not bank.find('a'):
            continue

        data_dict = {
            'Name': [bank],
            'TA_USD_Billion': [assets]
        }
        temp_df = pd.DataFrame(data_dict)
        df = pd.concat([df, temp_df], ignore_index=True)
        count += 1

    return df


def transform(df, rates):
    """Convert USD total assets values to various currencies."""
    df_rates = pd.read_csv(rates)
    rate_eur = float(df_rates.iloc[0, 1])
    rate_gbp = float(df_rates.iloc[1, 1])
    rate_inr = float(df_rates.iloc[2, 1])

    df.replace(',', '', regex=True, inplace=True)
    df = df.astype({'TA_USD_Billion': float})

    df['TA_GBP_Billion'] = np.round(df['TA_USD_Billion'] * rate_gbp, 2)
    df['TA_EUR_Billion'] = np.round(df['TA_USD_Billion'] * rate_eur, 2)
    df['TA_INR_Billion'] = np.round(df['TA_USD_Billion'] * rate_inr, 2)

    return df


def load_to_csv(df, filename):
    """Load transformed dataframe to .csv file."""
    df.to_csv(filename, index=False)


def load_to_db(df, table, connection):
    """Load transformed dataframe to .db file for future queries."""
    df.to_sql(table, connection, if_exists='replace', index=False)


def run_query(query, connection):
    """Run query on database, printing output."""
    print(query)
    output = pd.read_sql(query, connection)
    print(output)


log_progress("Preliminaries complete. Initiating ETL process")

extracted = extract(BANK_URL, INITIAL_ATTRIBUTES)
log_progress("Data extraction complete. Initiating transformation process")

transformed = transform(extracted, EXCHANGE_RATES)
log_progress("Data transformation complete. Initiating loading process")

load_to_csv(transformed, OUTPUT_CSV)
log_progress("Data saved to CSV file")

con = sqlite3.connect(DATABASE)
log_progress("SQL connection initiated")
load_to_db(transformed, TABLE, con)
log_progress("Data loaded to database as a table. Executing queries")

run_query("SELECT * FROM Largest_banks", con)
run_query("SELECT AVG(TA_GBP_Billion) FROM Largest_banks", con)
run_query("SELECT Name from Largest_banks LIMIT 5", con)

log_progress("Process complete")

con.close()
log_progress("Server connection closed")
