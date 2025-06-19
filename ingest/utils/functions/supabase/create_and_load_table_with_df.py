from psycopg2 import Error
from utils.functions.supabase.check_table_existence import check_table_existence
from utils.functions.supabase.infer_sql_type import infer_sql_type
import logging
import pandas as pd
import psycopg2


def create_and_load_table_with_df(
    connection: psycopg2.connect,
    df: pd.DataFrame,
    database_name: str,
    schema_name: str,
    table_name: str
):
    """
    Arguments:
    - connection: SQL database connection
    - df: Pandas dataframe
    - database_name: Database name
    - schema_name: Schema name
    - table_name: Table name

    Creates table (if not exists) and loads data (from dataframe)
    """
    logger = logging.getLogger(__name__)
    cursor = None

    try:
        # get column data types
        column_type_list = []
        column_list = []
        for col, dtype in df.dtypes.items():
            sql_type = infer_sql_type(dtype)
            col_quoted = f'"{col}"'
            column_list.append(col_quoted)
            column_type_list.append(f"{col_quoted} {sql_type}")

        # initialize cursor
        cursor = connection.cursor()

        table_exists_flag = check_table_existence(
            connection=connection,
            database_name=database_name,
            schema_name=schema_name,
            table_name=table_name
        )

        if table_exists_flag != True:

            # create table
            create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {database_name}.{schema_name}.{table_name} (
                    {', '.join(column_type_list)}
                )
            """
            cursor.execute(create_table_sql)
            # logger.info(f"Table created or already exists: {database_name}.{schema_name}.{table_name}")

        # insert data
        insert_sql = f"""
            INSERT INTO {database_name}.{schema_name}.{table_name} ({', '.join(column_list)})
            VALUES ({', '.join(['%s'] * len(column_list))})
        """
        cursor.executemany(insert_sql, df.values.tolist())
        # logger.info(f"Inserted {len(df)} records into {database_name}.{schema_name}.{table_name}")

    except Exception as ex:
        logger.error(f"Unexpected error: {ex}")
        connection.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
