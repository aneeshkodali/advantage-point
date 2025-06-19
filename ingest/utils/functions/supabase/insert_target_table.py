from typing import (
    List,
)
import logging
import psycopg2

def insert_target_table(
    connection: psycopg2.connect,
    target_database_name: str,
    target_schema_name: str,
    target_table_name: str,
    source_database_name: str,
    source_schema_name: str,
    source_table_name: str,
    unique_column_name_list: List[str]
):
    """
    Arguments:
    - connection: Snowflake connection
    - target_database_name: Database name for target table
    - target_schema_name: Schema name for target table
    - target_table_name: Target table name
    - source_database_name: Database name for source table
    - source_schema_name: Schema name for source table
    - source_table_name: Source table name
    - unique_column_name_list: Column list that determines uniqueness among rows

    Compare records between source table and target table.
    Inserts records from source table that do not exist in the target table.
    """

    # Use a module-specific logger
    logger = logging.getLogger(__name__)

    # create cursor
    cursor = connection.cursor()

    # create alias variables (for use in SQL statements)
    target_alias = 'TGT'
    source_alias = 'SRC'

    # get list of columns from source table (for use in UPDATE statements)
    source_column_sql = f"""
        SELECT
            COLUMN_NAME
        FROM {source_database_name}.INFORMATION_SCHEMA.COLUMNS
        WHERE 1=1
            AND TABLE_SCHEMA = '{source_schema_name}'
            AND TABLE_NAME = '{source_table_name}'
    """
    cursor.execute(source_column_sql)
    source_column_list = [row[0] for row in cursor.fetchall()]

    # # generate lists/strings for unique/nonunique columns
    unique_column_join_str = ' AND '.join([
        f"{source_alias}.{unique_column_name} = {target_alias}.{unique_column_name}"
        for unique_column_name in unique_column_name_list
    ])
    unique_column_where_str_w_target_alias = ' OR '.join([
        f"{target_alias}.{unique_column_name} IS NULL"
        for unique_column_name in unique_column_name_list
    ])
    source_column_str = ',\n'.join(source_column_list)
    source_column_str_w_source_alias = ',\n'.join(
        [
            f"{source_alias}.{col}" for col in source_column_list
        ]
    )

    # handle inserts for new records
    insert_new_sql = f"""
        INSERT INTO {target_database_name}.{target_schema_name}.{target_table_name}
        ({source_column_str}, AUDIT_COLUMN__ACTIVE_FLAG, AUDIT_COLUMN__RECORD_TYPE, AUDIT_COLUMN__START_DATETIME_UTC, AUDIT_COLUMN__INSERT_DATETIME_UTC)
        SELECT
            {source_column_str_w_source_alias},
            TRUE AS AUDIT_COLUMN__ACTIVE_FLAG,
            'insert' AS AUDIT_COLUMN__RECORD_TYPE,
            CURRENT_TIMESTAMP AS AUDIT_COLUMN__START_DATETIME_UTC,
            CURRENT_TIMESTAMP AS AUDIT_COLUMN__INSERT_DATETIME_UTC
        FROM {source_database_name}.{source_schema_name}.{source_table_name} AS {source_alias}
        LEFT JOIN {target_database_name}.{target_schema_name}.{target_table_name} AS {target_alias} ON {unique_column_join_str}
        WHERE {unique_column_where_str_w_target_alias}
        ;
    """
    # logger.info(f"Inserting new records into: {target_database_name}.{target_schema_name}.{target_table_name}")
    # logger.debug(f"Running SQL: {insert_new_sql}")
    cursor.execute(insert_new_sql)

    # close cursor
    cursor.close()