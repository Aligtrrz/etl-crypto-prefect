# flows/etl_cryptos_flow.py

from datetime import datetime

import logging
from prefect import flow, task

from transform import transform_data
from save_to_csv import save_to_csv
from load_to_sql import load_data_to_sql


# Configuración básica de logging (Prefect también maneja logs propios)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="etl_cryptos_prefect.log",
    filemode="a"
)


@task(retries=3, retry_delay_seconds=10)
def extract_and_transform_task():
    """
    Task de Prefect que encapsula tu transform_data(),
    que a su vez ya hace la extracción + transformación.
    """
    logging.info("Task: Extrayendo y transformando datos de la API...")
    df = transform_data()

    if df is None or df.empty:
        logging.error("Task: DataFrame vacío luego de transform_data().")
        raise ValueError("DataFrame vacío en extract_and_transform_task")

    logging.info("Task: Transformación completada correctamente.")
    return df


@task
def save_to_csv_task(df):
    """
    Task de Prefect para guardar el CSV.
    """
    logging.info("Task: Guardando DataFrame a CSV (staging)...")
    save_to_csv(df, "cryptos_raw.csv")
    logging.info("Task: CSV guardado correctamente.")


@task
def load_to_sql_task(df):
    """
    Task de Prefect para cargar el DataFrame a SQL Server.
    """
    logging.info("Task: Preparando carga a SQL Server...")
    run_ts = datetime.now()
    load_data_to_sql(df, table_name="CryptosHistory", run_timestamp=run_ts)
    logging.info("Task: Carga a SQL Server completada.")


@flow(name="ETL_Cryptos_Flow")
def etl_cryptos_flow():
    """
    Flow principal de Prefect que orquesta las tareas.
    """
    logging.info("Flow: Iniciando flujo ETL de criptomonedas con Prefect...")

    # 1) Extraer + Transformar
    df = extract_and_transform_task()

    # 2) Guardar CSV (staging)
    save_to_csv_task(df)

    # 3) Cargar a SQL Server (histórico)
    load_to_sql_task(df)

    logging.info("Flow: Flujo ETL completado exitosamente.")


if __name__ == "__main__":
    etl_cryptos_flow()
