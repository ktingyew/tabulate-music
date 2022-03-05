import argparse
from datetime import datetime
import logging
import os
from pathlib import Path
import sys
import yaml

from google.cloud import bigquery as bq
import pandas as pd

from scan_library import full_scan, cached_scan, check_df_na

LOG_DIR = Path(os.environ['LOGS_TARGET'])
LIBRARY_DIR = Path(os.environ['LIBRARY_TARGET'])
REPORT_DIR = Path(os.environ['REPORT_TARGET'])

def main():

    # Get current datetime
    dt_now_str : str = datetime.now().strftime("%Y-%m-%d %H-%M-%S") # SQL Datetime format

    # Scan music library to df
    if FLAGS.fullscan:
        logger.info("Full Scan initiated. Not using cache.")
        df = full_scan(path_to_lib=LIBRARY_DIR)
    else:
        logger.info("Cached Scan initiated.")
        df = cached_scan(
            path_to_lib=LIBRARY_DIR,
            path_to_report_dir=REPORT_DIR
        )

    logger.info(
        f"Extracted {len(df)} songs from {LIBRARY_DIR}")

    # Check for na values in df
    check_df_na(df)

    # Save to Local: in newline delimited json format
    if not FLAGS.nolocal:
        df_out_path = f"{REPORT_DIR}/report {dt_now_str}.jsonl"
        df.to_json(df_out_path, force_ascii=False, orient='records', lines=True)
        logger.info(f"Saved df as json file to {df_out_path}")


    if not FLAGS.nobq:

        logger.info("Storing new table to BQ enabled.")

        # BigQuery env vars
        PROJECT_ID = os.environ['PROJECT_ID']
        DATASET_STORE_ID = os.environ['DATASET_STORE_ID']
        DATASET_LATEST_ID = os.environ['DATASET_LATEST_ID']
        TABLE_MUSIC_ID = os.environ['TABLE_MUSIC_ID']

        # Authenticate with BigQuery
        bq_client = bq.Client() # TODO: use production credentials
        logger.info(f"BigQuery. Successfully authenticated")

        # Load Schema from yaml file
        with open("./src/schema.yaml", "r") as stream:
            yaml_gen = yaml.safe_load_all(stream) # load generator
            _ = next(yaml_gen)
            _ = next(yaml_gen)
            bq_schema = next(yaml_gen)['bq_music_schema']
        schema = [
            bq.SchemaField(
                name=f.split(':')[0], 
                field_type=f.split(':')[1], 
                mode='NULLABLE'
            ) 
            for f in bq_schema.split(',')
        ]

        # Modify DateAdded type from "object" to "DateTime"
        # schema in bq is DATE; requires this to be in pd's datetime format
        df['DateAdded'] = pd.to_datetime(df['DateAdded'])

        # Add new table
        tbl_store_ref = bq.table.TableReference(
            dataset_ref=bq.dataset.DatasetReference(
                project=PROJECT_ID, 
                dataset_id=DATASET_STORE_ID
            ), 
            table_id=dt_now_str # current time as the table id
        )
        tbl_store = bq.Table(tbl_store_ref, schema=schema) 
        bq_client.create_table(tbl_store)
        bq_client.load_table_from_dataframe(df, tbl_store_ref)
        logger.info(f"load_table_from_dataframe to {tbl_store} successful")

        # Delete then upload new table in music dataset
        tbl_latest_ref = bq.table.TableReference(
            dataset_ref=bq.dataset.DatasetReference(
                project=PROJECT_ID, 
                dataset_id=DATASET_LATEST_ID
            ), 
            table_id=TABLE_MUSIC_ID
        )
        tbl_latest = bq.Table(tbl_latest_ref, schema=schema) 
        bq_client.delete_table(tbl_latest, not_found_ok=True)
        logger.info(f"{tbl_latest} deleted")
        bq_client.create_table(tbl_latest)
        logger.info(f"{tbl_latest} recreated")
        bq_client.load_table_from_dataframe(df, tbl_latest_ref)
        logger.info(f"load_table_from_dataframe to {tbl_latest} successful")
    


if __name__ == '__main__':

    # Parse arguments
    parser = argparse.ArgumentParser(description = 'Say hello')
    parser.add_argument('--fullscan', action="store_true", help='Option: Do not use cache when scanning')
    parser.add_argument('--nobq', action="store_true", help='Option: Do not upload to BigQuery')
    parser.add_argument('--nolocal', action="store_true", help='Option: Do not save to local disk')
    parser.add_argument('--nowritelog', action="store_true", help='Option: Do not write to log file')
    FLAGS = parser.parse_args()
    
    # Logging configuration
    logger = logging.getLogger("main")
    logger.setLevel(logging.DEBUG)
    fmtter = logging.Formatter(
        "[%(asctime)s]| %(levelname)8s| %(name)20s| %(message)s", 
        "%Y-%m-%d %H:%M:%S")
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(fmtter)
    stdout_handler.setLevel(logging.DEBUG)
    logger.addHandler(stdout_handler)
    if not FLAGS.nowritelog:
        file_handler = logging.FileHandler(LOG_DIR/"music.log", encoding='utf8')
        file_handler.setFormatter(fmtter)
        file_handler.setLevel(logging.DEBUG)
        logger.addHandler(file_handler)
    else: logger.warning("Write to logs disabled. ")

    # Replace sys.excepthook with one that also logs critical-level
    def my_excepthook(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        logger.critical("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))
    sys.excepthook = my_excepthook

    # Run main
    logger.info(f"STARTED: flags={FLAGS}")
    main()
    logger.info("COMPLETED: ")
