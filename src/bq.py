import logging
import os
import yaml

from google.cloud import bigquery as bq
import pandas as pd

 # BigQuery env vars
PROJECT_ID = os.environ['PROJECT_ID']
DATASET_ID = os.environ['DATASET_ID']
LIB_TABLE_ID = os.environ['LIB_TABLE_ID']
DIFF_TABLE_ID = os.environ['DIFF_TABLE_ID']

# Authenticate with BigQuery
bq_client = bq.Client() # TODO: use production credentials

logger = logging.getLogger("main.bq")



def bq_replace_lib_table(
    df: pd.DataFrame
) -> None :

    # Construct uri string to BigQuery table
    LIB_TABLE_REF_STR: str = f"{PROJECT_ID}.{DATASET_ID}.{LIB_TABLE_ID}"

    # Load schema from yaml file
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

    # Modify DateAdded and Report_Time type from "object" to "DateTime"
    # schema in bq is DATE; requires this to be in pd's datetime format
    df['DateAdded'] = pd.to_datetime(df['DateAdded'], format="%Y-%m-%d")
    df['Report_Time'] = pd.to_datetime(df['Report_Time'], format="%Y-%m-%d %H:%M:%S")

    bq_client.load_table_from_dataframe(
        dataframe=df, 
        destination=LIB_TABLE_REF_STR,
        job_config=bq.job.LoadJobConfig(
            schema=schema,
            write_disposition="WRITE_TRUNCATE"
        )
    )
    logger.info(f"load_table_from_dataframe to {LIB_TABLE_REF_STR} successful")

def bq_append_diff_table(
    df: pd.DataFrame
) -> None :

    # Immediately return if there's no diff
    if len(df) == 0:
        return

    # Construct uri string to BigQuery table
    DIFF_TABLE_REF_STR: str = f"{PROJECT_ID}.{DATASET_ID}.{DIFF_TABLE_ID}"

    bq_schema = "op:STRING,id:INTEGER,field_name:STRING,field_type:STRING,old_val:STRING,new_val:STRING,datetime:DATETIME,remarks:STRING"

    schema = [
        bq.SchemaField(
            name=f.split(':')[0], 
            field_type=f.split(':')[1], 
            mode='NULLABLE'
        ) 
        for f in bq_schema.split(',')
    ]


    # Modify datime's type from "object" to "DateTime"
    # schema in bq is DATE; requires this to be in pd's datetime format
    df['datetime'] = pd.to_datetime(df['datetime'], format="%Y-%m-%d %H-%M-%S")

    bq_client.load_table_from_dataframe(
        dataframe=df, 
        destination=DIFF_TABLE_REF_STR,
        job_config=bq.job.LoadJobConfig(
            schema=schema,
            write_disposition="WRITE_APPEND"
        )
    )
    logger.info(f"load_table_from_dataframe to {DIFF_TABLE_REF_STR} successful")