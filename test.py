from extract_cryptos import fetch_crypto_data
from transform import transform_data
import pandas as pd
import json
from save_to_csv import save_to_csv


df= transform_data()
save = save_to_csv(df, 'cryptos_raw.csv')

#test = fetch_crypto_data()

#data = transform_data()