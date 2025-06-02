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

    # construct query to check table existence
    table_exists_sql = f"""
        SELECT
            COUNT(*) > 0 AS TABLE_EXISTS_FLAG
        FROM {database_name}.INFORMATION_SCHEMA.TABLES
        WHERE
                TABLE_SCHEMA = '{schema_name}'
            AND TABLE_NAME = '{table_name}'
        ;
    """

    # execute query for target table existence
    cursor.execute(table_exists_sql)
    table_exists_flag = cursor.fetchone()[0]

    if table_exists_flag == True:
        # drop table if exists
        drop_table_sql = f"DROP TABLE {database_name}.{schema_name}.{table_name}"
        # logger.debug(f"Running SQL: {drop_table_sql}")
        cursor.execute(drop_table_sql)
        # logger.info(f"Dropped table: {database_name}.{schema_name}.{table_name}")
        

    # close cursor
    cursor.close()