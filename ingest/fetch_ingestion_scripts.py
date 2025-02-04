from ingest.utils.functions.sql import (
    create_connection,
)
import json
import logging
import os

def main():

    # set logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # get schema
    schema_meta = os.getenv('SCHEMA_META')
    
    # create connection
    conn = create_connection()

    # create cursor
    cursor = conn.cursor()

    # get script names from table
    script_select_sql = f"""
        SELECT
            script_name
        FROM {schema_meta}.ingestion_scripts
        WHERE is_active = true
    """
    logging.info(f"Running select statement: {script_select_sql}")
    cursor.execute(script_select_sql)
    script_list = [
        {'script_name': row[0]} for row in cursor.fetchall()
    ]

    # close connection
    cursor.close()
    conn.close()

    # store json output in GitHub workflow format
    scripts_json = json.dumps(script_list)
    github_output = os.getenv('GITHUB_ENV') # get environment file path
    with open(github_output, 'a') as env_file:
        env_file.write(f"SCRIPTS_JSON={scripts_json}\n")
    # print(f"::set-output name=scripts_json::{scripts_json}")

if __name__ == "__main__":
    main()