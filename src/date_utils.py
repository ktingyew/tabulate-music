from datetime import datetime
import glob
import os
import pathlib
import re

def find_file_with_latest_dt_in_dir(
    directory: pathlib.Path,
    re_search: str = r"\b20.*00",
    dt_formatter: str = "%Y-%m-%d %H-%M-%S",
    ext = "*.jsonl"

) -> pathlib.Path :
    """ Identify file in dir with the most recent dt in its filename
    
    Used to identify the latest report available. So that it can be
    loaded as a cache. 
    """

    dt_ls = []
    for f in glob.glob(str(directory/ext)):
        dt_ls.append(
            datetime.strptime(
                re.search(re_search, f).group(),
                dt_formatter
            )
        )

    dt_latest = max(dt_ls).strftime("%Y-%m-%d %H-%M-%S")

    return pathlib.Path(
        glob.glob(str(directory/f'*{dt_latest}*'))[0]
    )


def num_mins_elapsed_since_last_modified(
    filepath: pathlib.Path,
) -> int : 
    """ Find the no. of minutes (integer) since a file was last modified

    A file's last modified time is as reflected through the OS.
    It is obtained through os.path.getmtime.
    Does not need to access file's contents. 
    This is the basis fofr determining whether to read from cache or not.
    
    """
    ts: float = os.path.getmtime(filepath)
    tdelta: datetime.timedelta = datetime.now() - datetime.fromtimestamp(ts)
    return int(tdelta.total_seconds()/60)
