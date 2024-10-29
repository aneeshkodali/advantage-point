from dotenv import load_dotenv
from typing import (
    Any
)
import datetime
import logging
import os
import pandas as pd
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
    connection: psycopg2.connect,
    schema_name: str
):
    """
    Arguments:
    - connection: SQL database connection
    - schema_name: schema

    Creates schema if it does not exist
    """

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
    
    # close cursor
    cursor.close()

def infer_sql_type(python_dtype: Any):
    """
    Arguments:
    - python_dtype: python data type

    Return (postgres) sql data type based on the python data type passed
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
    sql_type = type_mapping_dict.get(type(python_dtype), 'TEXT')

    return sql_type

def create_and_load_table(
    connection: psycopg2.connect,
    df: pd.DataFrame,
    schema_name: str,
    table_name: str
):
    """
    Arguments:
    - connection: SQL database connection
    - df: Pandas dataframe
    - schema_name: Schema name
    - table_name: Table name

    Does the following tasks:
    - drop (if exists) and create table
    - insert data into table
    """

    # get column data types
    column_type_list = []
    column_list = []
    for col, dtype in df.dtypes.items():
        sql_type = infer_sql_type(dtype)
        column_list.append(col)
        column_type_list.append(f"{col} {sql_type}")

    # inititialize cursor
    cursor = connection.cursor()

    # drop table if exists
    drop_table_sql = f"DROP TABLE IF EXISTS {schema_name}.{table_name}"
    logging.info(f"Running statement: {drop_table_sql}")
    cursor.execute(drop_table_sql)

    # create table
    create_table_sql = f"CREATE TABLE {schema_name}.{table_name} ({',\n'.join(column_type_list)})"
    logging.info(f"Running statement: {create_table_sql}")
    cursor.execute(create_table_sql)

    # Insert data into table
    insert_sql = f"INSERT INTO {schema_name}.{table_name} ({', '.join(column_list)}) VALUES ({', '.join(['%s'] * len(column_list))})"
    logging.info(f"Running statement: {insert_sql}")

    # Use execute many for bulk insert
    cursor.executemany(insert_sql, df.values.tolist())
    connection.commit()

    # closer cursor
    cursor.close()

def create_or_alter_target_table(
    connection: psycopg2,
    target_schema_name: str,
    target_table_name: str,
    source_schema_name: str,
    source_table_name: str
):
    """
    Arguments:
    - connection: SQL database connection
    - target_schema_name: Schema name for target table
    - target_table_name: Target table name
    - source_schema_name: Schema name for source table
    - source_table_name: Source table name

    Based on source table columns, creates target table if it does not exist or alters target table columns
    """

    # create cursor
    cursor = connection.cursor()

    # check if target table exists
    target_table_exists_sql = f"""
        SELECT
            COUNT(*) > 0 AS target_table_exists_flag
        FROM INFORMATION_SCHEMA.TABLES
        WHERE
                TABLE_SCHEMA = '{target_schema_name}'
            AND TABLE_NAME = '{target_table_name}'
        ;
    """

    # execute query for target table existence
    logging.info(f"Executing statement: {target_table_exists_sql}")
    cursor.execute(target_table_exists_sql)
    target_table_exists_flag = cursor.fetchone()[0]
    logging.info(f"Target table {target_schema_name}.{target_table_name} exists: {target_table_exists_flag}")

    # create or alter target tabl
    if target_table_exists_flag:
        pass
    else:
        pass




