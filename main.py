import argparse
from datetime import datetime
import logging.config
import os
from pathlib import Path
import sys

from src.scan_library import full_scan, cached_scan, check_df_na
from src.bq import replace_bq_table

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

    # Write to bq, replacing the table
    if not FLAGS.nobq:
        replace_bq_table(df=df)


if __name__ == '__main__':

    # Parse arguments
    parser = argparse.ArgumentParser(description = 'Say hello')
    parser.add_argument('--fullscan', action="store_true", help='Option: Do not use cache when scanning')
    parser.add_argument('--nobq', action="store_true", help='Option: Do not upload to BigQuery')
    parser.add_argument('--nolocal', action="store_true", help='Option: Do not save to local disk')
    parser.add_argument('--nowritelog', action="store_true", help='Option: Do not write to log file')
    FLAGS = parser.parse_args()
    
    # Logging configuration using file
    logging.config.fileConfig(
        fname="logging.config", 
        defaults={'logfilename': LOG_DIR/'music.log'}
    )
    logger = logging.getLogger('main')
    if FLAGS.nowritelog:
        logger.handlers = [ h for h in logger.handlers if not isinstance(h, logging.FileHandler) ]
        logger.warning("Write to logs disabled. ")

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
