from dotenv import load_dotenv
from utils.functions.version_control import get_current_branch
import logging
import os
import psycopg2

def create_connection():
    """
    Create sql connection
    """

    # get current branch
    branch = get_current_branch()
    print(branch)

    # import environment variables
    if branch == 'master':
        load_dotenv('utils/config/.env.prod')
    else:
        load_dotenv('utils/config/.env.dev')

    conn = psycopg2.connect(
        dbname=os.getenv('DATABASE'),
        user=os.getenv('USER'),
        password=os.getenv('PASSWORD'),
        host=os.getenv('HOST'),
        port=os.getenv('PORT')
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
    logging.info(f"Ensuring schema exists: {schema_name}")
    logging.info(f"Executing statement:\n {create_schema_sql}")
    cursor.execute(create_schema_sql)
    conn.commit()
    logging.info(f"{schema_name} exists.")

    # close
    cursor.close()
    conn.close()