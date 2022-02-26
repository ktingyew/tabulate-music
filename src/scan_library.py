import logging
import os
import pathlib
import threading
import yaml

import pandas as pd

from date_utils import find_file_with_latest_dt_in_dir, num_mins_elapsed_since_last_modified
from tag_extractor import song_tag_extractor

logger = logging.getLogger("main.scan_library")

# Load Schemas
with open("./src/schema.yaml", "r") as stream:
    yaml_gen = yaml.safe_load_all(stream) # load generator
    
    pd_schema_init = next(yaml_gen) # schema 1
    pd_schema_completion = next(yaml_gen) # schema 2


def check_df_na(
    df: pd.DataFrame
) -> None :
    """ Check for na values in music DataFrame on a certain list of columns.
    This function is read-only; does not modify df contents.

    When na value is found, it is logged.   
    """

    for col in [
        'Title', 'Artist', 'Album_Artist', 'Album', 'Major_Genre', 'BPM', 
        'Key', 'Year', 'Rating', 'Major_Language' , 'Gender', 'DateAdded', 
        'Time', 'Bitrate', 'Extension', 'Filename'
        ]:
        if len(df[df[col].isna()]) > 0:
            # filter records with missing values in column c
            subdf = df[df[col].isna()][['Title', 'Artist']].values 
            for ks in subdf:
                logger.warning(f"Column `{col}` contains null value: {ks}")



def full_scan(
    path_to_lib: pathlib.Path
) -> pd.DataFrame :
    """

    Schema:
        - Title: object
        - Artist: object
        - Album_Artist: object
        - Album: object
        - Major_Genre: object
        - Minor_Genre: object
        - BPM: int64
        - Key: object
        - Year: int64
        - Rating: float64
        - Major_Language: object
        - Minor_Language: object
        - Gender: object
        - DateAdded: object
        - Energy: Int64
        - KPlay: Int64
        - Time: float64
        - Bitrate: int64
        - Extension: object
        - Filename: object

    Remarks:
        - `DateAdded` type as `object` is intentional
            - It will be changed to datetime prior to uploading to bq
            - Reason not to change to datetime type immediately, because 
              preserving string/object type allows for readability when 
              saving as .jsonl file
        - `int64` and `Int64` are NOT the same types:
            - int64: Does not support null values
            - Int64: Support null values

    Returns:
        pd.DataFrame containing the songs and their tags
    """

    records = []

    def _full_scan_thread(fpath: pathlib.Path):
        """ Target function for a thread to execute
        
        Args:
            fpath: Full path to (music) file
        """
        try:
            records.append(song_tag_extractor(fpath))
        except NotImplementedError:
            logger.warning(
                "Invalid file format (not .mp3/.flac) detected in" \
                + f" {fpath}"
            )

    thread_ls = []
    for f in os.listdir(path_to_lib)[:]:

        th = threading.Thread(
            target=_full_scan_thread, 
            args=(path_to_lib/f, ))
        thread_ls.append(th)
        th.start()

    for th in thread_ls:
        th.join()

    df = pd.DataFrame.from_records(records)
    df = df.astype(pd_schema_init).astype(pd_schema_completion) 

    return df


def cached_scan(
    path_to_lib: pathlib.Path,
    path_to_report_dir: pathlib.Path,
    num_mins_thres: int = 5 * 24 * 60
) -> pd.DataFrame :
    """

    Schema: See fn full_scan() above

    Remarks: See fn full_scan() above

    Returns:
        pd.DataFrame containing the songs and their tags
    """

    cache = _get_cache_df(path_to_report_dir)

    records = []

    for f in os.listdir(path_to_lib)[:]:

        try:
            cached_tags: dict = cache.loc[f].to_dict()

            if num_mins_elapsed_since_last_modified(path_to_lib/f) < num_mins_thres:
                records.append(song_tag_extractor(path_to_lib/f))

            else:
                records.append(cached_tags)

        except KeyError: # not cached
            records.append(song_tag_extractor(path_to_lib/f))
            logger.info(f"Not in cache: Possible new song found: {f}")


    df = pd.DataFrame.from_records(records)
    df = df.astype(pd_schema_init).astype(pd_schema_completion) 
    # df['DateAdded'] = pd.to_datetime(df['DateAdded']) 

    return df



def _get_cache_df(
    path_to_report_dir: pathlib.Path
) -> pd.DataFrame :
    """
    """

    # Get latest cache, load as df
    fpath = find_file_with_latest_dt_in_dir(
        directory=path_to_report_dir,
        re_search=r"\b20.*-\d\d",
        ext="*.jsonl"
    )
    logger.debug(f"Cache retrieved from {fpath}")

    cache = pd.read_json(
        fpath, 
        orient='records', 
        convert_dates=False, 
        lines=True) 

    cache = cache.set_index(
        keys='Filename',
        drop=False,
        append=False
    )

    return cache
