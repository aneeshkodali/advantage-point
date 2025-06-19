from typing import (
    Dict,
)
from utils.functions.supabase.alter_target_table import alter_target_table
from utils.functions.supabase.check_table_existence import check_table_existence
from utils.functions.supabase.create_and_load_table_with_df import create_and_load_table_with_df
from utils.functions.supabase.create_target_table import create_target_table
from utils.functions.supabase.drop_table import drop_table
from utils.functions.supabase.get_distinct_table_id_list import get_distinct_table_id_list
from utils.functions.supabase.insert_target_table import insert_target_table
from utils.functions.supabase.update_target_table import update_target_table
from utils.functions.web.tennisabstract.matches.get_match_data_df import get_match_data_df
from utils.functions.web.tennisabstract.matches.get_match_url_list import get_match_url_list as get_match_url_list__tennisabstract
import logging
import psycopg2

def main(
    connection: psycopg2.connect,
    table_record_dict: Dict
):

    # set logging
    logging.basicConfig(
        level=logging.info,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)


    # parse record dict
    is_incremental_load_enabled = table_record_dict['is_incremental_load_enabled']
    target_database_name = table_record_dict['target_database_name']
    target_schema_name = table_record_dict['target_schema_name']
    target_table_name = table_record_dict['target_table_name']
    temp_database_name = table_record_dict['temp_database_name']
    temp_schema_name = table_record_dict['temp_schema_name']
    temp_table_name = table_record_dict['temp_table_name']    
    source_max_record_load_count = table_record_dict['source_max_record_load_count']
    source_unique_column_list = table_record_dict['source_unique_column_list']

    # get list of records from source
    source_record_list = get_match_url_list__tennisabstract()

    # filter record list (if incremental enabled)
    if is_incremental_load_enabled == True and target_table_exists_flag == True:
        # get url list from table
        db_record_list = get_distinct_table_id_list(
            connection=connection,
            database_name=target_database_name,
            schema_name=target_schema_name,
            table_name=target_table_name,
            id_column_name_list=source_unique_column_list,
            where_clause_list=['audit_column__active_flag = true',]
        )

        # filter list
        record_list = [
            record_dict for record_dict in source_record_list
            if record_dict not in db_record_list
        ]
    
    else:
        record_list = source_record_list

    logger.info(f"Number of source records found: {len(record_list)}")

    # loop through matches (use batching) -> create df
    logger.info(f"Loading data to table: {temp_database_name}.{temp_schema_name}.{temp_table_name}")
    for i in range(0, len(record_list), source_max_record_load_count):

        record_batch_list = record_list[i:i+source_max_record_load_count]

        start_idx = i + 1
        end_idx = min(i + source_max_record_load_count, len(record_list))

        logger.info(
            f"Processing records {start_idx} to {end_idx} out of {len(record_list)} "
            f"(batch {i // source_max_record_load_count + 1})"
        )

        # drop temp table
        logger.info(f"Ensuring table is dropped: {temp_database_name}.{temp_schema_name}.{temp_table_name}")
        drop_table(
            connection=connection,
            database_name=temp_database_name,
            schema_name=temp_schema_name,
            table_name=temp_table_name
        )
        logger.info(f"Table no longer exists: {temp_database_name}.{temp_schema_name}.{temp_table_name}")        

        # creating and loading temp table
        logger.info(f"Loading data to table: {temp_database_name}.{temp_schema_name}.{temp_table_name}")
        record_batch_df = get_match_data_df(
            match_url_list = record_batch_list
        )
        create_and_load_table_with_df(
            connection=connection,
            df=record_batch_df,
            database_name=temp_database_name,
            schema_name=temp_schema_name,
            table_name=temp_table_name
        )
        logger.info(f"Data loaded to table: {temp_database_name}.{temp_schema_name}.{temp_table_name}")

        # check if target table exists
        target_table_exists_flag = check_table_existence(
            connection=connection,
            database_name=target_database_name,
            schema_name=target_schema_name,
            table_name=target_table_name
        )
        logger.info(f"Target table {target_database_name}.{target_schema_name}.{target_table_name} exists: {target_table_exists_flag}")
        
        if target_table_exists_flag == True:
            
            # handle column alterations
            logger.info(f"Handling schema drift in target table: {target_database_name}.{target_schema_name}.{target_table_name}")
            alter_target_table(
                connection=connection,
                target_database_name=target_database_name,
                target_schema_name=target_schema_name,
                target_table_name=target_table_name,
                source_database_name=temp_database_name,
                source_schema_name=temp_schema_name,
                source_table_name=temp_table_name
            )

            # handle (Type II) updates
            logger.info(f"Handling updates in target table: {target_database_name}.{target_schema_name}.{target_table_name}")
            update_target_table(
                connection=connection,
                target_database_name=target_database_name,
                target_schema_name=target_schema_name,
                target_table_name=target_table_name,
                source_database_name=temp_database_name,
                source_schema_name=temp_schema_name,
                source_table_name=temp_table_name,
                unique_column_name_list=source_unique_column_list
            )

        else:
            # handle table creation
            logger.info(f"Creating target table: {target_database_name}.{target_schema_name}.{target_table_name}")
            create_target_table(
                connection=connection,
                target_database_name=target_database_name,
                target_schema_name=target_schema_name,
                target_table_name=target_table_name,
                source_database_name=temp_database_name,
                source_schema_name=temp_schema_name,
                source_table_name=temp_table_name
            )

        # handle inserts
        logger.info(f"Handling inserts in target table: {target_database_name}.{target_schema_name}.{target_table_name}")
        insert_target_table(
            connection=connection,
            target_database_name=target_database_name,
            target_schema_name=target_schema_name,
            target_table_name=target_table_name,
            source_database_name=temp_database_name,
            source_schema_name=temp_schema_name,
            source_table_name=temp_table_name,
            unique_column_name_list=source_unique_column_list
        )

        # drop temp table
        logger.info(f"Ensuring table is dropped at end: {temp_database_name}.{temp_schema_name}.{temp_table_name}")
        drop_table(
            connection=connection,
            database_name=temp_database_name,
            schema_name=temp_schema_name,
            table_name=temp_table_name
        )
        logger.info(f"Table no longer exists: {temp_database_name}.{temp_schema_name}.{temp_table_name}")

if __name__ == "__main__":
    main()