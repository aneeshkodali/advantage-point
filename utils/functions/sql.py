from dotenv import load_dotenv
from utils.functions.version_control import get_current_branch
import logging
import os
import psycopg2

def create_connection():
    """
    Create sql connection
    """

    conn = psycopg2.connect(
        dbname=os.getenv('DATABASE'),
        user=os.getenv('USER'),
        password=os.getenv('PASSWORD'),
        host=os.getenv('HOST'),
        port=os.getenv('PORT')
    )
    
    return conn

def create_schema(
    connection,
    schema_name
):
    """
    Arguments:
    - connection: SQL database connection
    - schema_name: schema

    Creates schema if it does not exist
    """

    # set schema based on current branch
    branch = get_current_branch()
    schema_name_env = schema_name if branch == 'master' else f"{schema_name}_dev"

    # create cursor
    cursor = connection.cursor()

    # generate sql statement
    create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema_name_env}"

    # execute statement
    logging.info(f"Ensuring schema exists: {schema_name_env}")
    logging.info(f"Executing statement:\n {create_schema_sql}")
    cursor.execute(create_schema_sql)
    connection.commit()
    logging.info(f"Schema exists: {schema_name_env}.")

    # close cursor
    cursor.close()
