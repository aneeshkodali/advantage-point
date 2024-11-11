from utils.functions.sql import (
    create_and_load_table,
    create_connection,
    create_or_alter_target_table,
    create_schema,
    merge_target_table
)
from utils.functions.version_control import get_current_branch
from warehouse.ingest.ingest_players import main as ingest_players
from warehouse.ingest.ingest_tournaments import main as ingest_tournaments
import logging
import os
import pandas as pd

def main():

    # set logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # create connection
    conn = create_connection()

    # set schema based on current branch
    branch = get_current_branch()
    
    # create ingest schemas
    logging.info(f"Creating ingestion layer schemas.")
    schema_ingest = os.getenv('SCHEMA_INGESTION')
    schema_ingest = schema_ingest if branch == 'master' else f"{schema_ingest}_dev"
    schema_ingest_temp = f"{os.getenv('SCHEMA_INGESTION')}_temp"
    schema_ingest_temp = schema_ingest_temp if branch == 'master' else f"{schema_ingest_temp}_dev"

    for schema in [schema_ingest, schema_ingest_temp]:
        create_schema(
            connection=conn,
            schema_name=schema
        )

    logging.info("Starting data ingestion.")

    # create configuration dictionary - used on ALL sources
    config_dict = {
        'temp_schema': schema_ingest_temp,
        'target_schema': schema_ingest,
        'connection': conn,
    }

    # generate list of sources
    source_list = [
        # {
        #     'source_name': 'tournaments',
        #     'fn_ingest_data': ingest_tournaments,
        #     'unique_column_list': ['tournament_url'],
        # },
        {
            'source_name': 'players',
            'fn_ingest_data': ingest_players,
            'unique_column_list': ['player_url'],
            'alter_table_drop_column_flag': False,
            'merge_table_delete_row_flag': False,
        },
    ]

    # loop through sources
    for source_dict in source_list:

        # combine dictionaries
        source_config_dict = {
            **source_dict,
            **config_dict,
        }

        # parse dictionary properties
        source_name = source_dict['source_name']
        fn_ingest_data = source_dict['fn_ingest_data']

        logging.info(f"Starting ingestion: {source_name}")
        
        logging.info(f"Running ingestion for {source_name} with config: {source_config_dict}")

        # create dataframe
        source_data_list = fn_ingest_data()
        source_data_df = pd.DataFrame(source_data_list)

        # create or replace temp table and insert
        logging.info(f"Creating temp table for {source_name}.")
        create_and_load_table(
            connection=conn,
            df=source_data_df,
            schema_name=schema_ingest_temp,
            table_name=source_name
        )
        logging.info(f"Created temp table for {source_name}: {schema_ingest_temp}.{source_name}")

        # create or alter target table
        logging.info(f"Creating or altering target table for {source_name}.")
        create_or_alter_target_table(
            connection=conn,
            target_schema_name=schema_ingest,
            target_table_name=source_name,
            source_schema_name=schema_ingest_temp,
            source_table_name=source_name,
            drop_column_flag=source_config_dict['alter_table_drop_column_flag']
        )

        # merge into target table
        logging.info(f"Merging records into target table for {source_name}")
        merge_target_table(
            connection=conn,
            target_schema_name=schema_ingest,
            target_table_name=source_name,
            source_schema_name=schema_ingest_temp,
            source_table_name=source_name,
            unique_column_list=source_config_dict['unique_column_list'],
            delete_row_flag=source_config_dict['merge_table_delete_row_flag']
        )

        logging.info(f"Completed ingestion: {source_name}")


    logging.info("Data ingestion completed.")

    # close connection
    conn.close()

if __name__ == "__main__":
    main()