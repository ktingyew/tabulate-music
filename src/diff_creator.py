from datetime import datetime
import logging
import os
from pathlib import Path

import pandas as pd

from .date_utils import get_recent_df

REPORT_DIR = Path(os.environ['REPORT_TARGET'])

logger = logging.getLogger("main.diff_creator")

pdf_schema = {
    "ID": "int64",
    "Major_Genre": "object",
    "Minor_Genre": "object",
    "Rating": "float64",
    "KPlay": "float64",
    "Filename": "object"
}

pdf_schema_nullable = {
    "KPlay": "Int64"
}


def get_diff_pdf(
    new: pd.DataFrame
) -> pd.DataFrame:
    """

    """

    tag_bq_type: dict = {
        "Major_Genre": "STRING",
        "Minor_Genre": "STRING",
        "Rating": "FLOAT",
        "KPlay": "INTEGER"
    }

    old = get_old_df_for_diff()

    new = refit_new_df_for_diff(new)
    
    dt_now_str : str = datetime.now().strftime("%Y-%m-%d %H-%M-%S") 

    OPERATIONS = []
    
    old_extraneous: list = [ x for x in old.index.tolist() if x not in new.index.tolist() ]
    new_extraneous: list = [ x for x in new.index.tolist() if x not in old.index.tolist() ]

    # Add deletions to diff
    for i in old_extraneous:
        OPERATIONS.append({
            "op": "del",
            "id": i,
            "datetime": dt_now_str,
            "remarks": old.loc[i]['Filename']
        })

    # Add insertions to diff
    for i in new_extraneous:
        OPERATIONS.append({
            "op": "ins",
            "id": i,
            "datetime": dt_now_str,
            "remarks": new.loc[i]['Filename']
        })
    
    # Add updates to diff
    oldd = old.drop(labels=old_extraneous, axis='index').copy()
    neww = new.drop(labels=new_extraneous, axis='index').copy()

    for i in oldd.index:
        o = oldd.loc[i]
        n = neww.loc[i]
        d = o.compare(n, align_axis=1)

        for t in d.index:
            OPERATIONS.append({
                "op": "upd",
                "id": i,
                "field_name": t,
                "field_type": tag_bq_type[t],
                "old_val": d.loc[t]['self'],
                "new_val": d.loc[t]['other'],
                "datetime": dt_now_str,
                "remarks": new.loc[i]['Filename']
            })

    # Construct diff pandas df
    diff = pd.DataFrame.from_records(OPERATIONS)

    if len(diff) > 0:

        # Change pandas dataframe (diff) schema 
        diff_schema = {
            "op": "object",
            "id": "int64",
            "field_name": "object",
            "field_type": "object",
            "old_val": "object",
            "new_val": "object",
            "datetime": "object", # conversion to datetime will be handled by my bq.py module
            "remarks": "object"
        }
        diff = diff.astype(diff_schema)
        # Force conversion to string 
        diff['old_val'] = diff['old_val'].astype(str)
        diff['new_val'] = diff['new_val'].astype(str)

        # Log to debug level
        logger.debug(f"Number of new diff created: {len(diff)}")
        for i in range(len(diff)):
            logger.debug(f"diff {i}: {diff.iloc[i].to_dict()}")

    else:
        logger.debug("No diff created")

    return diff


def get_old_df_for_diff():

    old = get_recent_df(REPORT_DIR)
    old = old.set_index(
        keys='ID',
        drop=False,
        append=False
    )

    # Keep only certain tags (that we are tracking for diff)
    old = old[list(pdf_schema.keys())]
    return old.astype(pdf_schema).astype(pdf_schema_nullable)


def refit_new_df_for_diff(
    df: pd.DataFrame
):

    new = df.copy()
    new = new.set_index(
        keys='ID',
        drop=False,
        append=False
    )

    # Keep only certain tags (that we are tracking for diff)
    new = new[list(pdf_schema.keys())]
    return new.astype(pdf_schema).astype(pdf_schema_nullable)
