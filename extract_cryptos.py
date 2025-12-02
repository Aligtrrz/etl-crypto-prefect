import requests
import json

def fetch_crypto_data(vs_currency="usd", per_page=10):
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": vs_currency,
        "order": "market_cap_desc",
        "per_page": per_page,
        "page": 1,
        "sparkline": False
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()

    else:
        raise Exception(f"Error fetching data: {response.status_code}")
    
    json_formateado = json.dumps(
        data,
        indent=4)
    #print(json_formateado)

    return data