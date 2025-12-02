from extract_cryptos import fetch_crypto_data
import pandas as pd
import json


def transform_data():

    data = fetch_crypto_data()
    if not data:
        print("⚠️ No se recibieron datos desde la API.")
        return pd.DataFrame()
    data_normalizada = []
    for crypto in data:
        registros={
            "id": crypto.get('id', None),
            "symbol": crypto.get('symbol', None),
            "name": crypto.get('name', None),
            "current_price": crypto.get('current_price', None),
            "market_cap": crypto.get('market_cap', None),
            "total_volume": crypto.get('total_volume', None)
        }
        data_normalizada.append(registros)
    df = pd.DataFrame(data_normalizada)

    columnas_numericas = ["current_price", "market_cap", "total_volume"]
    for columna in columnas_numericas:
        df[columna] = pd.to_numeric(df[columna], errors='coerce')
    #print(data[0])
    #print(df)
    return df