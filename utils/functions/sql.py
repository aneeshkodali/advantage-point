from dotenv import load_dotenv
import datetime
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

def create_and_load_table(
    connection,
    df,
    schema_table_name
):
    """
    Arguments:
    - connection: SQL database connection
    - df: Pandas dataframe
    - schema_table_name: schema_name.table_name

    Does the following tasks:
    - drop (if exists) and create table
    - insert data into table
    """

    # get column data types
    column_type_list = []
    for col, dtype in df.dtypes.items():
        sql_type = infer_sql_type(dtype)
        column_type_list.append(f"{col} {sql_type}")

    # inititialize cursor
    cursor = connection.cursor()

    # drop table if exists
    drop_table_sql = f"DROP TABLE IF EXISTS {schema_table_name}"
    logging.info(f"Running statement: {drop_table_sql}")
    cursor.execute(drop_table_sql)

    # create table
    create_table_sql = f"CREATE TABLE {schema_table_name} ({',\n'.join(column_type_list)})"
    logging.info(f"Running statement: {create_table_sql}")
    cursor.execute(create_table_sql)

    # insert data into table
    values = ", ".join(
        f"({', '.join(map(repr, row))})" for row in df.values.tolist()
    )
    insert_sql = f"""
        INSERT INTO {schema_table_name} ({', '.join(column_type_list)})
        VALUES
        {values}
    """
    cursor.execute(insert_sql)


