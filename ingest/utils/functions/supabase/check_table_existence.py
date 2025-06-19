from psycopg2 import Error
import logging
import psycopg2


def check_table_existence(
    connection: psycopg2.connect,
    database_name: str,
    schema_name: str,
    table_name: str
) -> bool:
    """
    Arguments:
    - connection: SQL database connection
    - database_name: Database name
    - schema_name: Schema name
    - table_name: Table name

    Checks if table exists
    """
    logger = logging.getLogger(__name__)
    cursor = None

    try:

        # initialize cursor
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

        # execute query for table existence
        cursor.execute(table_exists_sql)
        table_exists_flag = cursor.fetchone()[0]

        return table_exists_flag

    except Exception as ex:
        logger.error(f"Unexpected error: {ex}")
        raise

    finally:
        if cursor:
            cursor.close()
