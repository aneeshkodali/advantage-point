from dotenv import load_dotenv
from typing import (
    Any,
    Dict,
    List
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

    # create connection
    conn = psycopg2.connect(
        dbname=os.getenv('DATABASE'),
        user=os.getenv('USER'),
        password=os.getenv('PASSWORD'),
        host=os.getenv('HOST'),
        port=os.getenv('PORT')
    )

    # create cursor
    cursor = conn.cursor()

    # allow write operations
    conn.autocommit = True
    cursor.execute("SET session characteristics AS transaction READ WRITE;")
    cursor.execute("SET default_transaction_read_only = 'off';")

    # close cursor
    cursor.close()
    
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

def get_table_column_list(
    connection: psycopg2.connect,
    schema_name: str,
    table_name: str,
    column_name_list: List[str],
    where_clause_list: List[str] = ['1 = 1']
) -> List[Dict]:
    """
    Arguments:
    - conn: SQL connection
    - schema_name: Schema name
    - table_name: Table name
    - column_name_list: List of column names
    - where_clause_list: List of WHERE clause strings

    Returns column values as a list
    """

    # create cursor
    cursor = connection.cursor()

    # create sql-like strings from list
    column_name_join = ', '.join(column_name_list)
    where_clause_join = ' AND '.join([f"({where_clause})" for where_clause in where_clause_list])

    # create sql statement to retrieve records for columns
    select_sql = f"""
        SELECT DISTINCT
            {column_name_join}
        FROM {schema_name}.{table_name}
        WHERE {where_clause_join}
    """
    logging.info(f"Running select statement: {select_sql}")

    try:
    
        # store results as list of dicts
        cursor.execute(select_sql)
        results = cursor.fetchall()
        select_list = [dict(zip(column_name_list, row)) for row in results]

        return select_list

    except Exception as e:
        logging.info(f"Error executing statement: {e}")
        # roll back transaction to reset its state
        connection.rollback()
        return []
    
    finally:
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

def drop_table(
    connection: psycopg2.connect,
    schema_name: str,
    table_name: str
):
    """
    Arguments:
    - connection: SQL database connection
    - schema_name: Schema name
    - table_name: Table name

    Drop table if exists
    """

    # inititialize cursor
    cursor = connection.cursor()

    # drop table if exists
    drop_table_sql = f"DROP TABLE IF EXISTS {schema_name}.{table_name}"
    logging.info(f"Running statement: {drop_table_sql}")
    cursor.execute(drop_table_sql)
    logging.info(f"Dropped table: {schema_name}.{table_name}")

    # close cursor
    cursor.close()

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

    Creates table (if not exists) and loads data (from dataframe)
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

    # create table
    create_table_sql = f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} ({', '.join(column_type_list)})"
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
    connection: psycopg2.connect,
    target_schema_name: str,
    target_table_name: str,
    source_schema_name: str,
    source_table_name: str,
    drop_column_flag: bool
):
    """
    Arguments:
    - connection: SQL database connection
    - target_schema_name: Schema name for target table
    - target_table_name: Target table name
    - source_schema_name: Schema name for source table
    - source_table_name: Source table name
    - drop_column_flag: True/false flag to determine column deletion from target table (true)

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

    # create or alter target table
    if target_table_exists_flag:
        logging.info(f"Altering target table: {target_schema_name}.{target_table_name}")
        alter_target_table(
            connection=connection,
            target_schema_name=target_schema_name,
            target_table_name=target_table_name,
            source_schema_name=source_schema_name,
            source_table_name=source_table_name,
            drop_column_flag=drop_column_flag
        )
        logging.info(f"Target table altered as needed: {target_schema_name}.{target_table_name}")

    else:
        logging.info(f"Creating target table: {target_schema_name}.{target_table_name}")
        create_target_table(
            connection=connection,
            target_schema_name=target_schema_name,
            target_table_name=target_table_name,
            source_schema_name=source_schema_name,
            source_table_name=source_table_name
        )
        logging.info(f"Target table created: {target_schema_name}.{target_table_name}")

    # close cursor
    cursor.close()

def create_target_table(
    connection: psycopg2.connect,
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

    Based on source table columns, creates target table
    """

    # create cursor
    cursor = connection.cursor()

    # generate sql statemet to get source table column names and data types
    source_table_columns_sql = f"""
        WITH
            -- select columns from source table
            columns_source AS (
                SELECT
                    CONCAT(COLUMN_NAME, ' ', DATA_TYPE) AS column_name_data_type
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE
                        TABLE_SCHEMA = '{source_schema_name}'
                    AND TABLE_NAME = '{source_table_name}'
            ),
            columns_concat AS (
                -- combine columns into string
                SELECT
                    STRING_AGG(column_name_data_type, ', ') AS column_name_data_type_agg
                FROM columns_source
            ),
            final AS (
                SELECT * FROM columns_concat
            )
        SELECT * FROM final
            
    """

    # execute query for source table columns
    logging.info(f"Executing statement: {source_table_columns_sql}")
    cursor.execute(source_table_columns_sql)
    column_name_data_type_agg = cursor.fetchone()[0]

    # generate sql to create target table (includes audit fields)
    create_target_table_sql = f"""
        CREATE TABLE {target_schema_name}.{target_table_name}
        (
            {column_name_data_type_agg},
            audit_field_active_flag BOOLEAN,
            audit_field_record_type TEXT,
            audit_field_start_datetime_utc TIMESTAMP,
            audit_field_end_datetime_utc TIMESTAMP,
            audit_field_insert_datetime_utc TIMESTAMP,
            audit_field_update_datetime_utc TIMESTAMP,
            audit_field_delete_datetime_utc TIMESTAMP
        )
    """

    # execute query for target table creation
    logging.info(f"Executing statement: {create_target_table_sql}")
    cursor.execute(create_target_table_sql)
    connection.commit()

    # close cursor
    cursor.close()

    
def alter_target_table(
    connection: psycopg2.connect,
    target_schema_name: str,
    target_table_name: str,
    source_schema_name: str,
    source_table_name: str,
    drop_column_flag: bool
):
    """
    Arguments:
    - connection: SQL database connection
    - target_schema_name: Schema name for target table
    - target_table_name: Target table name
    - source_schema_name: Schema name for source table
    - source_table_name: Source table name
    - drop_column_flag: True/false flag to determine column deletion from target table (true)

    Based on source table columns, creates target table
    """

    # create cursor
    cursor = connection.cursor()

    # generate query to compare columns between source and target tables
    columns_compare_sql = f"""
        WITH
            -- select columns from source table
            columns_source AS (
                SELECT
                    COLUMN_NAME,
                    DATA_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE
                        TABLE_SCHEMA = '{source_schema_name}'
                    AND TABLE_NAME = '{source_table_name}'
            ),
            -- select columns from target table
            columns_target AS (
                SELECT
                    COLUMN_NAME,
                    DATA_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE
                        TABLE_SCHEMA = '{target_schema_name}'
                    AND TABLE_NAME = '{target_table_name}'
            ),
            -- compare columns
            columns_joined AS (
                SELECT
                    columns_target.COLUMN_NAME AS target_column_name,
                    columns_target.DATA_TYPE AS target_data_type,
                    columns_source.COLUMN_NAME AS source_column_name,
                    columns_source.DATA_TYPE AS source_data_type,
                    CASE
                        WHEN (columns_target.COLUMN_NAME IS NULL) AND (columns_source.COLUMN_NAME IS NULL) THEN 'No Change'
                        WHEN (columns_target.COLUMN_NAME IS NULL) THEN 'Add'
                        WHEN (columns_source.COLUMN_NAME IS NULL) AND ({drop_column_flag} = TRUE) THEN 'Drop'
                        WHEN (columns_target.DATA_TYPE != columns_source.DATA_TYPE) THEN 'Alter'
                        ELSE 'No Change'
                    END AS column_comparison_type
                FROM columns_target
                FULL OUTER JOIN columns_source ON columns_target.COLUMN_NAME = columns_source.COLUMN_NAME
            ),
            -- generate ALTER TABLE statements
            alter_table_statements AS (
                SELECT
                    *,
                    CONCAT(
                        'ALTER TABLE ', '{target_schema_name}', '.', '{target_table_name}', ' ',
                        CASE
                            WHEN column_comparison_type = 'Add' THEN CONCAT('ADD ', source_column_name, ' ', source_data_type)
                            WHEN column_comparison_type = 'Alter' THEN CONCAT('ALTER COLUMN ', target_column_name, ' ', source_data_type)
                            WHEN column_comparison_type = 'Drop' THEN CONCAT('DROP COLUMN ', target_column_name)
                            ELSE NULL
                        END
                    ) AS alter_table_statement
                FROM columns_joined
                WHERE column_comparison_type != 'No Change'
            ),
            final AS (
                SELECT * FROM alter_table_statements
            )
        SELECT * FROM final
    """

    # execute query for column_comparisons
    logging.info(f"Executing statement: {columns_compare_sql}")
    cursor.execute(columns_compare_sql)
    columns_compare_results = cursor.fetchall()

    # convert results to list of dictionaries instead of list of tuples
    columns_compare_results_keys = [desc[0] for desc in cursor.description]
    columns_compare_results_list = [dict(zip(columns_compare_results_keys, row)) for row in columns_compare_results]

    # loop through list and execute ALTER TABLE statement
    for columns_compare_result in columns_compare_results_list:

        # parse out ALTER TABLE statement
        alter_table_statement = columns_compare_result['alter_table_statement']
        logging.info(f"Running statement: {alter_table_statement}")
        cursor.execute(alter_table_statement)

    connection.commit()

    # close cursor
    cursor.close()


def merge_target_table(
    connection: psycopg2.connect,
    target_schema_name: str,
    target_table_name: str,
    source_schema_name: str,
    source_table_name: str,
    unique_column_list: List[str],
    delete_row_flag: bool
):
    """
    Arguments:
    - connection: SQL database connection
    - target_schema_name: Schema name for target table
    - target_table_name: Target table name
    - source_schema_name: Schema name for source table
    - source_table_name: Source table name
    - delete_row_flag: True/false flag to determine row deletion from target table (true)

    Based on source table rows, handles row inserts, updates, deletes for target table
    """

    # create cursor
    cursor = connection.cursor()

    # create alias variables (for use in SQL statements)
    target_alias = 'tgt'
    source_alias = 'src'

    # get list of columns from source table (for use in INSERT/UPDATE statements)
    source_column_sql = f"""
        SELECT
            COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE
                TABLE_SCHEMA = '{source_schema_name}'
            AND TABLE_NAME = '{source_table_name}'
    """
    cursor.execute(source_column_sql)
    source_column_list = [row[0] for row in cursor.fetchall()]

    # generate strings for unique/nonunique columns
    source_column_str = ', '.join(source_column_list)
    source_column_str_w_source_alias = ', '.join([f"{source_alias}.{col}" for col in source_column_list])
    unique_column_concat_str = "CONCAT_WS('_', " + ', '.join(unique_column_list) + ")"
    unique_column_concat_str_w_target_alias = "CONCAT_WS('_', " + ', '.join([f"{target_alias}.{col}" for col in unique_column_list]) + ")"
    unique_column_concat_str_w_source_alias = "CONCAT_WS('_', " + ', '.join([f"{source_alias}.{col}" for col in unique_column_list]) + ")"
    non_unique_column_list = list(filter(lambda col: col not in unique_column_list, source_column_list))
    non_unique_column_concat_str = "CONCAT_WS('_', " + ', '.join(non_unique_column_list) + ")"


    # create view to store comparison results
    comparison_schema_name = source_schema_name
    comparison_table_name = f"compare_{target_table_name}"
    create_comparison_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {comparison_schema_name}.{comparison_table_name} AS
            WITH
                {target_alias} AS (
                    SELECT
                        {unique_column_concat_str} AS unique_id,
                        {non_unique_column_concat_str} AS compare_id
                    FROM {target_schema_name}.{target_table_name}
                    WHERE audit_field_active_flag = TRUE
                ),
                {source_alias} AS (
                    SELECT
                        {unique_column_concat_str} AS unique_id,
                        {non_unique_column_concat_str} AS compare_id
                    FROM {source_schema_name}.{source_table_name}
                ),
                joined AS (
                    SELECT
                        {target_alias}.unique_id AS target_unique_id,
                        {target_alias}.compare_id AS target_compare_id,
                        {source_alias}.unique_id AS source_unique_id,
                        {source_alias}.compare_id AS source_compare_id,
                        CASE
                            WHEN 1=1
                                AND ({target_alias}.unique_id IS NULL)
                                AND ({source_alias}.unique_id IS NOT NULL) 
                            THEN 'insert'
                            WHEN 1=1
                                AND ({source_alias}.unique_id IS NULL)
                                AND ({target_alias}.unique_id IS NOT NULL)
                                AND ({delete_row_flag} = TRUE)
                            THEN 'delete'
                            WHEN 1=1
                                AND ({target_alias}.unique_id = {source_alias}.unique_id)
                                AND ({target_alias}.compare_id != {source_alias}.compare_id) 
                            THEN 'update'
                            ELSE 'no change'
                        END AS row_comparison
                        FROM {target_alias}
                        FULL OUTER JOIN {source_alias} USING (unique_id)
                )
            SELECT
                *
            FROM joined
            WHERE row_comparison != 'no change'
    """
    logging.info(f"Running create comparison table sql: {create_comparison_table_sql}")
    cursor.execute(create_comparison_table_sql)

    # handle deletes
    delete_sql = f"""
        UPDATE {target_schema_name}.{target_table_name} AS {target_alias}
        SET
            audit_field_active_flag = FALSE,
            audit_field_record_type = 'delete',
            audit_field_end_datetime_utc = NOW(),
            audit_field_delete_datetime_utc = NOW()
        FROM {comparison_schema_name}.{comparison_table_name} AS compare
        WHERE 1=1
            AND ({target_alias}.audit_field_active_flag = TRUE)
            AND {unique_column_concat_str_w_target_alias} = compare.target_unique_id
            AND compare.row_comparison = 'delete'
    """
    logging.info(f"Running delete statement: {delete_sql}")
    cursor.execute(delete_sql)

    # handle updates
    update_existing_sql = f"""
        UPDATE {target_schema_name}.{target_table_name} AS {target_alias}
        SET
                audit_field_active_flag = FALSE,
                audit_field_end_datetime_utc = NOW(),
                audit_field_update_datetime_utc = NOW()
            FROM {comparison_schema_name}.{comparison_table_name} AS compare
            WHERE 1=1
                AND ({target_alias}.audit_field_active_flag = TRUE)
                AND {unique_column_concat_str_w_target_alias} = compare.target_unique_id
                AND compare.row_comparison = 'update'
    """
    logging.info(f"Running update existing statement: {update_existing_sql}")
    cursor.execute(update_existing_sql)

    update_new_sql = f"""
        INSERT INTO {target_schema_name}.{target_table_name} ({source_column_str}, audit_field_active_flag, audit_field_record_type, audit_field_start_datetime_utc, audit_field_insert_datetime_utc)
        SELECT
            {source_column_str_w_source_alias},
            TRUE AS audit_field_active_flag,
            'update' AS audit_field_record_type,
            NOW() AS audit_field_start_datetime_utc,
            NOW() AS audit_field_insert_datetime_utc
        FROM {source_schema_name}.{source_table_name} AS {source_alias}
        LEFT JOIN {comparison_schema_name}.{comparison_table_name} AS compare
        ON {unique_column_concat_str_w_source_alias} = compare.source_unique_id
        WHERE compare.row_comparison = 'update'
    """
    logging.info(f"Running update new statement: {update_new_sql}")
    cursor.execute(update_new_sql)

    # handle inserts
    insert_sql = f"""
        INSERT INTO {target_schema_name}.{target_table_name} ({source_column_str}, audit_field_active_flag, audit_field_record_type, audit_field_start_datetime_utc, audit_field_insert_datetime_utc)
        SELECT
            {source_column_str_w_source_alias},
            TRUE AS audit_field_active_flag,
            'insert' AS audit_field_record_type,
            NOW() AS audit_field_start_datetime_utc,
            NOW() AS audit_field_insert_datetime_utc
        FROM {source_schema_name}.{source_table_name} AS {source_alias}
        LEFT JOIN {comparison_schema_name}.{comparison_table_name} AS compare
        ON {unique_column_concat_str_w_source_alias} = compare.source_unique_id
        WHERE compare.row_comparison = 'insert'
    """
    logging.info(f"Running insert statement: {insert_sql}")
    cursor.execute(insert_sql)

    # drop comparison table
    drop_comparison_table_sql = f"""
        DROP TABLE {comparison_schema_name}.{comparison_table_name}
    """
    logging.info(f"Running drop comparison table sql: {drop_comparison_table_sql}")
    cursor.execute(drop_comparison_table_sql)

    # commit
    connection.commit()

    # close cursor
    cursor.close()

def ingest_df_to_sql(
    connection: psycopg2.connect,
    df: pd.DataFrame,
    target_schema_name: str,
    target_table_name: str,
    temp_schema_name: str,
    temp_table_name: str,
    unique_column_list: List[str],
    drop_column_flag: bool,
    delete_row_flag: bool
):
    """
    Arguments:
    - connection: SQL database connection
    - df: Pandas dataframe
    - target_schema_name: Schema name for target table
    - target_table_name: Target table name
    - temp_schema_name: Schema name for temp table
    - temp_table_name: Temp table name
    - unique_column_list: List of fields that define uniqueness
    - drop_column_flag: True/false flag to determine column deletion from target table (true)
    - delete_row_flag: True/false flag to determine row deletion from target table (true)

    Ingests dataframe data into database:
    - create temp table using dataframe data
    - create or alter target table using temp table schema
    - merge records from temp table into target table, handling inserts, (type II) updates, deletes
    """

    # convert null values to SQL-compatible null values
    df = df.where(pd.notnull(df), None)
    
    # drop temp table
    drop_table(
        connection=connection,
        schema_name=temp_schema_name,
        table_name=temp_table_name
    )
    
    # create temp table
    create_and_load_table(
        connection=connection,
        df=df,
        schema_name=temp_schema_name,
        table_name=temp_table_name
    )

    # create or alter target table
    create_or_alter_target_table(
        connection=connection,
        target_schema_name=target_schema_name,
        target_table_name=target_table_name,
        source_schema_name=temp_schema_name,
        source_table_name=temp_table_name,
        drop_column_flag=drop_column_flag
    )

    # merge into target table
    merge_target_table(
        connection=connection,
        target_schema_name=target_schema_name,
        target_table_name=target_table_name,
        source_schema_name=temp_schema_name,
        source_table_name=temp_table_name,
        unique_column_list=unique_column_list,
        delete_row_flag=delete_row_flag
    )
