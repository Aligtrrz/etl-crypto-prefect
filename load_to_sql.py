import pyodbc
from datetime import datetime



def get_connection():
    server = r'ALIGTRRZ-V3\ALIGTRRZ_DB'
    database = 'CryptosDB'
    username = 'sa'
    password = 'Yerald.31'  

    conn_str = f"""
        DRIVER={{ODBC Driver 17 for SQL Server}};
        SERVER={server};
        DATABASE={database};
        UID={username};
        PWD={password};
        TrustServerCertificate=yes;
    """
    return pyodbc.connect(conn_str)


def ensure_table_exists(conn, table_name: str):
    """
    Crea la tabla si no existe.
    """
    cursor = conn.cursor()

    # 👇 Ajusta los tipos según lo que quieras en tu DW
    create_table_sql = f"""
    IF NOT EXISTS (
        SELECT * FROM sys.tables WHERE name = '{table_name}'
    )
    BEGIN
        CREATE TABLE {table_name} (
            id              VARCHAR(255),
            symbol          VARCHAR(50),
            name            VARCHAR(100),
            current_price   FLOAT,
            market_cap      FLOAT,
            total_volume    FLOAT,
            loaded_at       DATETIME
        );
    END
    """

    cursor.execute(create_table_sql)
    cursor.close()
    conn.commit()


def get_last_record(cursor, table_name: str, crypto_id: str):
    """
    Devuelve el último registro histórico de una cripto (por id),
    o None si nunca se ha cargado antes.
    """
    select_sql = f"""
        SELECT TOP 1 current_price, market_cap, total_volume
        FROM {table_name}
        WHERE id = ?
        ORDER BY loaded_at DESC;
    """

    cursor.execute(select_sql, crypto_id)
    row = cursor.fetchone()

    if row is None:
        return None

    # row[0] = current_price, row[1] = market_cap, row[2] = total_volume
    return {
        "current_price": row[0],
        "market_cap": row[1],
        "total_volume": row[2]
    }


def load_data_to_sql(df, table_name: str = "CryptosHistory", run_timestamp=None):
    """
    Carga el contenido de un DataFrame a una tabla en SQL Server.
    Solo inserta una nueva fila si cambió al menos uno de los valores
    (current_price, market_cap, total_volume) respecto al último registro.
    """

    if df is None or df.empty:
        print("⚠️ El DataFrame está vacío. No hay datos para cargar.")
        return

    # Si no viene run_timestamp desde fuera, lo generamos aquí
    if run_timestamp is None:
        run_timestamp = datetime.now()

    conn = None
    try:
        conn = get_connection()
        print("✅ Conectado a SQL Server.")

        # 1. Asegurarse de que la tabla exista
        ensure_table_exists(conn, table_name)

        cursor = conn.cursor()

        # 2. Definir el INSERT
        insert_sql = f"""
            INSERT INTO {table_name} (id, symbol, name, current_price, market_cap, total_volume, loaded_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """

        inserted_rows = 0
        skipped_rows = 0

        # 3. Insertar fila por fila solo si cambió algo
        for _, row in df.iterrows():
            crypto_id = row["id"]

            # 3.1 Traer último registro histórico para esa cripto (si existe)
            last_record = get_last_record(cursor, table_name, crypto_id)

            # 3.2 Si existe un registro anterior, comparamos
            if last_record is not None:
                same_price = last_record["current_price"] == row["current_price"]
                same_cap = last_record["market_cap"] == row["market_cap"]
                same_vol = last_record["total_volume"] == row["total_volume"]

                # Si TODO es igual, no insertamos nada
                if same_price and same_cap and same_vol:
                    skipped_rows += 1
                    continue  # pasa a la siguiente cripto

            # 3.3 Si no existía registro previo o algo cambió, insertamos fila
            cursor.execute(
                insert_sql,
                row["id"],
                row["symbol"],
                row["name"],
                row["current_price"],
                row["market_cap"],
                row["total_volume"],
                run_timestamp
            )
            inserted_rows += 1

        conn.commit()
        cursor.close()

        print(f"✅ Datos cargados en {table_name}. Filas insertadas: {inserted_rows}, filas sin cambios: {skipped_rows}.")

    except pyodbc.Error as ex:
        print("❌ Error al cargar datos en SQL Server.")
        print(f"Detalles: {ex}")
    finally:
        if conn is not None:
            conn.close()
            print("🔌 Conexión cerrada.")