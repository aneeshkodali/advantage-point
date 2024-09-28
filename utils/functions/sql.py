from utils.config.sql import (
    DATABASE,
    HOST,
    PASSWORD,
    PORT,
    USER
)
import logging
import psycopg2

def create_connection():
    """
    Create sql connection
    """

    conn = psycopg2.connect(
        dbname=DATABASE,
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT
    )

    return conn

def create_schema(schema_name):
    """
    Arguments:
    - schema_name: schema

    Creates schema if it does not exist
    """

    # create connection
    conn = create_connection()

    # create cursor
    cursor = conn.cursor()

    # generate sql statement
    create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema_name}"

    # execute statement
    logging.info(f"Creating schema: {schema_name}")
    logging.info(f"Executing statement:\n {create_schema_sql}")
    cursor.execute(create_schema_sql)
    conn.commit()
    logging.info(f"{schema_name} created successfully.")

    # close
    cursor.close()
    conn.close()