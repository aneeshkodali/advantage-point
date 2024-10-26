from utils.functions.sql import (
    create_connection,
    create_schema
)
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
    
    # create ingest schemas
    logging.info(f"Creating ingestion layer schemas.")
    schema_ingest = os.getenv('SCHEMA_INGESTION')
    schema_ingest_temp = f"{schema_ingest}_temp"
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
        source_data_df = pd.Dataframe(source_data_list)

        # create or replace temp table and insert
        logging.info(f"Create temp table for {source_name}.")
        source_data_df.to_sql(
            name=f"{source_config_dict['temp_schema_name']}.{source_config_dict['source_name']}",
            con=source_config_dict['connection'],
            if_exists='replace',
            index=False
        )
        logging.info(f"Created temp table for {source_name}: {schema_ingest_temp}.{source_name}")


        # create or alter target table

        # merge into target table

        logging.info(f"Completed ingestion: {source_name}")


    logging.info("Data ingestion completed.")

    # close connection
    conn.close()

if __name__ == "__main__":
    main()