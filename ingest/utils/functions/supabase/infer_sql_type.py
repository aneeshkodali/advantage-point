from typing import Any
from datetime import datetime, date

def infer_sql_type(python_value: Any) -> str:
    """
    Arguments:
    - python_value: Sample value from which to infer the data type.

    Returns:
    - str: Corresponding PostgreSQL (Supabase) data type as a string.
    """
    type_mapping_dict = {
        bool: 'BOOLEAN',
        int: 'INTEGER',
        float: 'DOUBLE PRECISION',
        str: 'TEXT',
        datetime: 'TIMESTAMP',
        date: 'DATE',
        type(None): 'TEXT'  # Default nullable type
    }

    return type_mapping_dict.get(type(python_value), 'TEXT')
