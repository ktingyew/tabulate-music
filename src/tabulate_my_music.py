from datetime import datetime
import logging
import os
from pathlib import Path
import sys
import threading
import yaml

from google.cloud import bigquery as bq
import pandas as pd

# Allow import from parent folder
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from utils import flac_extractor, mp3_extractor

LOG_DIR = Path(os.environ['LOGS_TARGET'])
LIBRARY_DIR = Path(os.environ['LIBRARY_TARGET'])
REPORT_DIR = Path(os.environ['REPORT_TARGET'])

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
fmtter = logging.Formatter(
    "[%(asctime)s]; %(levelname)s; %(name)s; %(message)s", 
    "%Y-%m-%d %H:%M:%S")
file_handler = logging.FileHandler(LOG_DIR/"music.log", encoding='utf8')
file_handler.setFormatter(fmtter)
file_handler.setLevel(logging.DEBUG)
logger.addHandler(file_handler)
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setFormatter(fmtter)
stdout_handler.setLevel(logging.DEBUG)
logger.addHandler(stdout_handler)

def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logger.critical("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))

sys.excepthook = handle_exception

def main():

    logger.info("STARTED: =========================================")

    # Authenticate with BigQuery
    bq_client = bq.Client()
    logger.info(f"BigQuery. Successfully authenticated")

    # Load Schemas
    with open("./src/schema.yaml", "r") as stream:
        yaml_gen = yaml.safe_load_all(stream) # load generator
        
        pd_schema_init = next(yaml_gen) # schema 1
        pd_schema_completion = next(yaml_gen) # schema 2
        bq_schema = next(yaml_gen)['bq_music_schema']

    # Load music files' tags to DataFrame with threading
    RECORDS = []
    def append_record(fpath):
        if fpath.endswith('.flac'):
            RECORDS.append(flac_extractor(LIBRARY_DIR/fpath))
        elif fpath.endswith('.mp3'):
            RECORDS.append(mp3_extractor(LIBRARY_DIR/fpath))
        else:
            logger.warning(
                "Invalid file format (not .mp3/.flac) detected in " \
                + f"{LIBRARY_DIR}: {f}"
            )

    thread_ls = []
    m_ls = os.listdir(LIBRARY_DIR)[:]
    for f in m_ls:
        th = threading.Thread(target=append_record, args=(f,))
        thread_ls.append(th)
        th.start()

    for th in thread_ls:
        th.join()

    logger.info(
        f"Extracted {len(RECORDS)} "
        + f"songs successfully from {LIBRARY_DIR}")

    df = pd.DataFrame.from_records(RECORDS)
    df = df.astype(pd_schema_init).astype(pd_schema_completion) 
    df['DateAdded'] = pd.to_datetime(df['DateAdded']) 

    # rename columns to replace whitespaces with underscore 
    # (whitespace in col name is illegal in BigQuery)
    df.columns = [ col.replace(' ', '_') for col in df.columns.tolist() ]

    # Check for missing values in DataFrame
    for c in [
        'Title', 'Artist', 'Album_Artist', 'Album', 'Major_Genre', 'BPM', 
        'Key', 'Year', 'Rating', 'Major_Language' , 'Gender', 'DateAdded', 
        'Time', 'Bitrate', 'Extension'
        ]:
        if len(df[df[c].isna()]) > 0:
            # filter records with missing values in column c
            subdf = df[df[c].isna()][['Title', 'Artist']].values 
            for ks in subdf:
                logger.warning(f"Column `{c}` contains null value: {ks}")

    dt = datetime.now().strftime("%Y-%m-%d %H-%M-%S") # SQL Datetime format

    # Save to Local: in newline delimited json format
    df_out_path = f"{REPORT_DIR}/report {dt}.json"
    df.to_json(df_out_path, orient='records', lines=True)
    logger.info(f"Report: Saved successfully in {df_out_path}")


    # BigQuery
    PROJECT_ID = os.environ['PROJECT_ID']
    DATASET_STORE_ID = os.environ['DATASET_STORE_ID']
    DATASET_LATEST_ID = os.environ['DATASET_LATEST_ID']
    TABLE_MUSIC_ID = os.environ['TABLE_MUSIC_ID']

    schema = [
        bq.SchemaField(
            name=f.split(':')[0], 
            field_type=f.split(':')[1], 
            mode='NULLABLE'
        ) 
        for f in bq_schema.split(',')
    ]

    # Save a new table in storage dataset
    tbl_store_ref = bq.table.TableReference(
        dataset_ref=bq.dataset.DatasetReference(
            project=PROJECT_ID, 
            dataset_id=DATASET_STORE_ID
        ), 
        table_id=dt # current time as the table id
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
    

    logger.info("COMPLETED: =======================================")



if __name__ == '__main__':
    main()
