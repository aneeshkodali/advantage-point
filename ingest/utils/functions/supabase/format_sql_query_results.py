from psycopg2.extensions import cursor as Psycopg2Cursor
from typing import (
    List,
)

def format_sql_query_results(
    cursor: Psycopg2Cursor
) -> List:
    """
    Arguments:
    - cursor: psycopg2 cursor

    Formats Snowflake results (found in cursor) as list of dictionaries
    """

    cursor_key_list = [desc[0] for desc in cursor.description]
    cursor_results_list = [dict(zip(cursor_key_list, row)) for row in cursor.fetchall()]

    return cursor_results_list
