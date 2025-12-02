from transform import transform_data
from save_to_csv import save_to_csv
from load_to_sql import load_data_to_sql
import logging
from datetime import datetime

logging.basicConfig(level=logging.DEBUG, 
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    filename='etl_cryptos.log',filemode='a')

def main():
    
    logging.info("Iniciando pipeline ETL de criptomonedas...")

    # 1. EXTRAER
    logging.debug("Extrayendo datos de la API...")
    df = transform_data()  

    if df.empty:
        logging.error("No se pudo continuar: DataFrame vacío.")
        return
    
    # 2. GUARDAR EN CSV
    logging.info("Guardando archivo CSV...")
    save_to_csv(df, "cryptos_raw.csv")

    # 3. CARGAR EN SQL SERVER
    run_ts = datetime.now()
    logging.debug("Cargando datos a SQL Server...")
    load_data_to_sql(df, table_name="CryptosHistory", run_timestamp=run_ts)

    logging.info("Pipeline completado exitosamente.")


if __name__ == "__main__":
    main()

