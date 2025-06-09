import logging
import os
import psycopg2

def create_schema(
    connection: psycopg2.connect,
    schema_name: str
):
    """
    Arguments:
    - connection: postgres psycopg2 connection
    - schema_name: Schema name
    
    Create schema if it does not exist.
    """

    # Use a module-specific logger
    logger = logging.getLogger(__name__)

    # create cursor
    cursor = connection.cursor()
    
    # create schema
    create_schema_sql = f"""
        CREATE SCHEMA IF NOT EXISTS {schema_name};
    """
    cursor.execute(create_schema_sql)
    # logger.info(f"Schema created: {schema_name}")

    # close cursor
    cursor.close()