from utils.config.sql import (
    DATABASE,
    HOST,
    PASSWORD,
    PORT,
    USER
)
from psycopg2 import sql
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
    # uses sql to prevent sql injection
    create_schema_sql = sql.SQL(f"CREATE SCHEMA IF NOT EXISTS {sql.Identifier(schema_name)}")

    # execute statement
    cursor.execute(create_schema_sql)
    conn.commit()
    logging.info(f"{schema_name} created successfully.")

    # close
    cursor.close()
    conn.close()