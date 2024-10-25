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
    schema_name = schema_name if branch == 'master' else f"{schema_name}_dev"

    # create cursor
    cursor = connection.cursor()

    # write query to check schema existence
    schema_exists_sql = f"""
        SELECT
            COUNT(*) > 0 AS schema_exists_flag
        FROM INFORMATION_SCHEMA.SCHEMATA
        WHERE SCHEMA_NAME = '{schema_name}'
        ;
    """

    # execute query
    logging.info(f"Executing statement: {schema_exists_sql}")
    cursor.execute(schema_exists_sql)
    schema_exists_flag = cursor.fetchone()[0]
    logging.info(f"Schema {schema_name} exists: {schema_exists_flag}")

    # conditionally create schema
    if schema_exists_flag:
        logging.info(f"Schema {schema_name} already exists.")
    else:
        # generate sql statement
        create_schema_sql = f"CREATE SCHEMA {schema_name}"
        logging.info(f"Executing statement:\n {create_schema_sql}")
        cursor.execute(create_schema_sql)
        connection.commit()
    
    # close cursor
    cursor.close()

def infer_sql_type(value):
    """
    Arguments:
    - value: python value

    Return (postgres) sql data type based on the python data type of the value passed
    """

    #  construct dictionary of type mappings
    type_mapping_dict = {
        bool: 'BOOLEAN',
        datetime: 'TIMESTAMP',
        float: 'FLOAT',
        int: 'INTEGER',
        str: 'TEXT',
    }

    # look up sql type; default to TEXT
    sql_type = type_mapping_dict.get(type(value), 'TEXT')

    return sql_type