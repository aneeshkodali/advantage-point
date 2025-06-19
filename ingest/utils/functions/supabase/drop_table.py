import logging
import psycopg2


def drop_table(
    connection: psycopg2.connect,
    database_name: str,
    schema_name: str,
    table_name: str
):
    """
    Arguments:
    - connection: postgres psycopg2 connection
    - database_name: Database name
    - schema_name: Schema name
    - table_name: Table name

    Drop table if exists
    """

    # Use a module-specific logger
    logger = logging.getLogger(__name__)

    # inititialize cursor
    cursor = connection.cursor()

    # drop table if exists
    drop_table_sql = f"DROP TABLE IF EXISTS {database_name}.{schema_name}.{table_name};"
    # logger.debug(f"Running SQL: {drop_table_sql}")
    cursor.execute(drop_table_sql)
    # logger.info(f"Table no longer exists: {database_name}.{schema_name}.{table_name}")
        
    # close cursor
    cursor.close()