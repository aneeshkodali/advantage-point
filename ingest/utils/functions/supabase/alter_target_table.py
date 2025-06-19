from utils.functions.supabase.format_sql_query_results import format_sql_query_results
import logging
import psycopg2

def alter_target_table(
    connection: psycopg2.connect,
    target_database_name: str,
    target_schema_name: str,
    target_table_name: str,
    source_database_name: str,
    source_schema_name: str,
    source_table_name: str
):
    """
    Arguments:
    - connection: SQL connection
    - target_database_name: Database name for target table
    - target_schema_name: Schema name for target table
    - target_table_name: Target table name
    - source_database_name: Database name for source table
    - source_schema_name: Schema name for source table
    - source_table_name: Source table name

    Based on source table columns, add/update target table columns
    """

    # Use a module-specific logger
    logger = logging.getLogger(__name__)

    # create cursor
    cursor = connection.cursor()

    columns_compare_sql = f"""
    WITH
        -- query source table columns
        SOURCE_COLUMNS AS (
            SELECT
                COLUMN_NAME,
                DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE,
                DATETIME_PRECISION
            FROM {source_database_name}.INFORMATION_SCHEMA.COLUMNS
            WHERE 1=1
                AND TABLE_SCHEMA = '{source_schema_name}'
                AND TABLE_NAME = '{source_table_name}'
        ),

        -- get source columns with full data type string
        SOURCE_TYPED_COLUMNS as (
            SELECT
                *,
                CASE 
                    WHEN DATA_TYPE IN ('CHAR', 'VARCHAR', 'STRING', 'TEXT') THEN DATA_TYPE || '(' || CHARACTER_MAXIMUM_LENGTH || ')'
                    WHEN DATA_TYPE IN ('NUMBER', 'DECIMAL', 'NUMERIC') THEN
                        CASE 
                            WHEN NUMERIC_SCALE IS NOT NULL THEN DATA_TYPE || '(' || NUMERIC_PRECISION || ',' || NUMERIC_SCALE || ')'
                            WHEN NUMERIC_PRECISION IS NOT NULL THEN DATA_TYPE || '(' || NUMERIC_PRECISION || ')'
                            ELSE DATA_TYPE
                        END
                    WHEN DATA_TYPE IN ('FLOAT', 'FLOAT4', 'FLOAT8', 'DOUBLE', 'DOUBLE PRECISION', 'REAL') THEN
                        CASE 
                            WHEN NUMERIC_PRECISION IS NOT NULL THEN DATA_TYPE || '(' || NUMERIC_PRECISION || ')'
                            ELSE DATA_TYPE
                        END
                    WHEN DATA_TYPE LIKE '%TIMESTAMP%' AND DATETIME_PRECISION IS NOT NULL THEN DATA_TYPE || '(' || DATETIME_PRECISION || ')'
                    ELSE DATA_TYPE
                END AS DATA_TYPE_FULL
            FROM SOURCE_COLUMNS
        ),

        -- query target table columns
        -- exclude audit columns
        TARGET_COLUMNS AS (
            SELECT
                COLUMN_NAME,
                DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE,
                DATETIME_PRECISION
            FROM {target_database_name}.INFORMATION_SCHEMA.COLUMNS
            WHERE 1=1
                AND TABLE_SCHEMA = '{target_schema_name}'
                AND TABLE_NAME = '{target_table_name}'
                AND COLUMN_NAME NOT IN (
                    'AUDIT_COLUMN__ACTIVE_FLAG',
                    'AUDIT_COLUMN__RECORD_TYPE',
                    'AUDIT_COLUMN__START_DATETIME_UTC',
                    'AUDIT_COLUMN__END_DATETIME_UTC',
                    'AUDIT_COLUMN__INSERT_DATETIME_UTC',
                    'AUDIT_COLUMN__UPDATE_DATETIME_UTC',
                    'AUDIT_COLUMN__DELETE_DATETIME_UTC'
                )
        ),

        -- compare columns between source and target
        COLUMNS_COMPARE AS (
            SELECT
                TARGET.COLUMN_NAME AS TARGET_COLUMN_NAME,
                TARGET.DATA_TYPE AS TARGET_DATA_TYPE,
                TARGET.CHARACTER_MAXIMUM_LENGTH AS TARGET_CHARACTER_MAXIMUM_LENGTH,
                TARGET.NUMERIC_PRECISION AS TARGET_NUMERIC_PRECISION,
                TARGET.NUMERIC_SCALE AS TARGET_NUMERIC_SCALE,
                TARGET.DATETIME_PRECISION AS TARGET_DATETIME_PRECISION,

                SOURCE.COLUMN_NAME AS SOURCE_COLUMN_NAME,
                SOURCE.DATA_TYPE AS SOURCE_DATA_TYPE,
                SOURCE.CHARACTER_MAXIMUM_LENGTH AS SOURCE_CHARACTER_MAXIMUM_LENGTH,
                SOURCE.NUMERIC_PRECISION AS SOURCE_NUMERIC_PRECISION,
                SOURCE.NUMERIC_SCALE AS SOURCE_NUMERIC_SCALE,
                SOURCE.DATETIME_PRECISION AS SOURCE_DATETIME_PRECISION,
                SOURCE.DATA_TYPE_FULL AS SOURCE_DATA_TYPE_FULL,

                CASE
                    WHEN TARGET.COLUMN_NAME IS NULL THEN 'add'
                    WHEN
                        TARGET.DATA_TYPE = SOURCE.DATA_TYPE
                        AND (
                            SOURCE.CHARACTER_MAXIMUM_LENGTH > TARGET.CHARACTER_MAXIMUM_LENGTH
                            OR (SOURCE.NUMERIC_PRECISION > TARGET.NUMERIC_PRECISION OR SOURCE.NUMERIC_SCALE > TARGET.NUMERIC_SCALE)
                            or SOURCE.DATETIME_PRECISION > TARGET.DATETIME_PRECISION
                        )
                    THEN 'alter'
                    ELSE NULL
                END AS COMPARISON_TYPE
            FROM TARGET_COLUMNS AS TARGET
            FULL OUTER JOIN SOURCE_TYPED_COLUMNS AS SOURCE ON TARGET.COLUMN_NAME = SOURCE.COLUMN_NAME
        ),

        -- construct ALTER TABLE statements
        ALTER_TABLE_STATEMENTS as (
            SELECT
                COLUMNS_COMPARE.*,
                CASE 
                    WHEN COMPARISON_TYPE = 'add'
                    THEN CONCAT(
                        'ALTER TABLE {target_database_name}.{target_schema_name}.{target_table_name} ADD ',
                        SOURCE_COLUMN_NAME,
                        ' ',
                        SOURCE_DATA_TYPE_FULL
                    )
                    WHEN COMPARISON_TYPE = 'alter'
                    THEN CONCAT(
                        'ALTER TABLE {target_database_name}.{target_schema_name}.{target_table_name} ALTER COLUMN ',
                        TARGET_COLUMN_NAME,
                        ' SET DATA TYPE ',
                        SOURCE_DATA_TYPE_FULL
                    )
                    ELSE NULL
                END AS ALTER_TABLE_STATEMENT
            FROM COLUMNS_COMPARE
            WHERE COMPARISON_TYPE IS NOT NULL
        )
    SELECT * FROM ALTER_TABLE_STATEMENTS
    ;
    """

    # execute query for column_comparisons
    # logger.debug(f"Running SQL: {columns_compare_sql}")
    cursor.execute(columns_compare_sql)

    # get results as list
    alter_statement_list = format_sql_query_results(
        cursor=cursor
    )
    logger.info(f"Found {len(alter_statement_list)} column changes to apply to target table: {target_database_name}.{target_schema_name}.{target_table_name}")

    # if not alter_statement_list:
    #     logger.info(f"No schema drift detected for {target_database_name}.{target_schema_name}.{target_table_name}")


    # loop through list and execute ALTER TABLE statement
    for alter_statement_dict in alter_statement_list:

        # parse out ALTER TABLE statement
        alter_table_statement = alter_statement_dict['ALTER_TABLE_STATEMENT']
        logger.info(f"Running statement: {alter_table_statement}")
        cursor.execute(alter_table_statement)

    # close cursor
    cursor.close()