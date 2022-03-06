import logging
import os
import yaml

from google.cloud import bigquery as bq
import pandas as pd

 # BigQuery env vars
PROJECT_ID = os.environ['PROJECT_ID']
DATASET_ID = os.environ['DATASET_ID']
TABLE_ID = os.environ['TABLE_ID']

# Construct uri string to BigQuery table
TABLE_REF_STR: str = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# Authenticate with BigQuery
bq_client = bq.Client() # TODO: use production credentials

logger = logging.getLogger("main.bq")

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

def replace_bq_table(
    df: pd.DataFrame
) -> None :

    # Modify DateAdded and Report_Time type from "object" to "DateTime"
    # schema in bq is DATE; requires this to be in pd's datetime format
    df['DateAdded'] = pd.to_datetime(df['DateAdded'])
    df['Report_Time'] = pd.to_datetime(df['Report_Time'])

    bq_client.load_table_from_dataframe(
        dataframe=df, 
        destination=TABLE_REF_STR,
        job_config=bq.job.LoadJobConfig(
            schema=schema,
            write_disposition="WRITE_TRUNCATE"
        )
    )
    logger.info(f"load_table_from_dataframe to {TABLE_REF_STR} successful")

