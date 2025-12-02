import pandas as pd
#from transform import transform_data

def save_to_csv(df: pd.DataFrame, filename: str):
    try:
        df.to_csv(filename, index=False, sep=';')
        print(f"✅ Datos guardados exitosamente en {filename}")
    except Exception as e:
        print(f"❌ Error al guardar los datos en CSV: {e}")

    return df
#save = save_to_csv('df', 'cryptos_data.csv')