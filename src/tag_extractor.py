from datetime import datetime
import pathlib
from typing import Tuple

from mutagen.flac import FLAC
from mutagen.mp3 import MP3, EasyMP3 as EMP3


def song_tag_extractor(
    filepath: pathlib.Path
) -> dict:
    """ Extracts tags of interest of a music file into a dictionary.
    Supported types: .flac, .mp3.

    This function DOES NOT modify the tags of the file. It is read-only. 

    Args:
        filepath: A pathlib.Path object of absolute path to .flac file. 
    
    Returns: 
        A dictionary containing the following (20) keys:
        - Title: str
        - Artist: str
        - Album_Artist: str
        - Album: str
        - Major_Genre: str
        - Minor_Genre: str
        - BPM: str
        - Key: str
        - Year: str
        - Rating: float
        - Major_Language: str
        - Minor_Language: str
        - Gender: str
        - DateAdded: str
        - Energy: str
        - KPlay: str
        - Time: float
        - Bitrate: int
        - Extension: str
        - Filename: str

        The values of each key in the dictionary (as of current implementation)
        contains are all un-nested.

    Raises:
        ValueError: If filepath does not lead to supported file extension.
    
    """
    if filepath.suffix == '.flac':
        return _flac_extractor(filepath)
    elif filepath.suffix == '.mp3':
        return _mp3_extractor(filepath)
    else:
        raise NotImplementedError("does not support non .flac or .mp3 files")


def _flac_extractor(
    filepath: pathlib.Path
) -> dict :
    """ Extracts tags of interest of a .flac file into a dictionary.

    """
    
    file = FLAC(f"{filepath}")
    out = {}
    
    mapper = {
        'Title': 'title',
        'Artist': 'artist',
        'Album_Artist': 'albumartist',
        'Album': 'album',
        'Genre': 'genre',
        'BPM': 'bpm',
        'Key': 'initial key',
        'Year': 'date',
        'Rating': 'rating',
        'Language': 'language',
        'Gender': 'copyright',
        'DateAdded': 'encodingtime',
        'Energy': 'energy',
        'KPlay': 'kplay'
    }
    
    for Tag in mapper.keys():
        try:
            t = file[mapper[Tag]]
            
            if Tag == 'Artist':
                out['Artist'] = "; ".join(t)
            
            elif Tag == 'Genre':
                if len(t) == 2:
                    out['Major_Genre'], out['Minor_Genre'] = t[0], t[1]
                else:
                    out['Major_Genre'], out['Minor_Genre'] = t[0], None
                    
            elif Tag == 'Language':
                if len(t) == 2:
                    out['Major_Language'], out['Minor_Language'] = t[0], t[1]
                else:
                    out['Major_Language'], out['Minor_Language'] = t[0], None

            elif Tag == 'Rating':
                out['Rating'] = float(t[0]) / 20.0
                
            elif Tag == 'DateAdded':
                out['DateAdded'] = \
                    datetime.strptime(t[0], '%d/%m/%Y').strftime("%Y-%m-%d")
                      
            else: # all other Tags
                out[Tag] = t[0]
                
        # Some tags are empty. Like 'energy' and 'kplay'. 
        # So an except block to catch these and give them None value.
        except:
            out[Tag] = None
            
    out['Time'] = file.info.length
    out['Bitrate'] = file.info.bitrate
    out['Extension'] = 'flac'
    out['Filename'] = filepath.name

    return out

def _mp3_extractor(
    filepath: pathlib.Path
) -> dict:
    """ Extracts tags of interest of a .mp3 file into a dictionary.

    """
    
    out = {}
    
    file = EMP3(f"{filepath}")
    
    mapper = {
        'Title': 'title',
        'Artist': 'artist',
        'Album_Artist': 'albumartist',
        'Album': 'album',
        'Genre': 'genre',
        'BPM': 'bpm',
        'Year': 'date',
        'Language': 'language',
        'Gender': 'copyright',
    }
    
    for Tag in mapper.keys():
        try:
            t = file[mapper[Tag]]
            
            if Tag == 'Artist':
                out['Artist'] = "; ".join(t)
            
            elif Tag == 'Genre':
                if len(t) == 2:
                    out['Major_Genre'], out['Minor_Genre'] = t[0], t[1]
                else:
                    out['Major_Genre'], out['Minor_Genre'] = t[0], None
                    
            elif Tag == 'Language':
                if len(t) == 2:
                    out['Major_Language'], out['Minor_Language'] = t[0], t[1]
                else:
                    out['Major_Language'], out['Minor_Language'] = t[0], None
                
            elif Tag == 'Rating':
                out[Tag] = t[0] / 20.0
            else: # all other Tags
                out[Tag] = t[0]
        except KeyError:
            out[Tag] = None

    # MP3 != EMP3. MP3 is more "dirty" compared to EMP3, but it has everything. 
    file = MP3(f"{filepath}")  

    out['Energy'] = out['DateAdded'] = out['KPlay'] = None
    for t in file.tags.getall('TXXX'):
        if   t.desc == 'EnergyLevel':
            out['Energy'] = t.text[0]
        elif t.desc == 'ENCODINGTIME':
            out['DateAdded'] = datetime.strptime(t.text[0], '%d/%m/%Y').strftime("%Y-%m-%d")
        elif t.desc == 'KPLAY':
            out['KPlay'] = t.text[0]
        else:
            pass

    out['Key'] = file.tags.getall('TKEY')[0].text[0]
    
    def _mp3rating(mutagen_mp3):
        """ Converts mp3 internal rating-values to proper no. of stars. 

        This is a simple helper function meant to specfically adress the weird
        rating storage in .mp3 files. It has no need to exist outside of this
        scope.
        
        Args:
            mutagen_mp3: mutagen.mp3.MP3

        Returns:
            A float value, representing the number of stars rated in this song,
             following the 5-star rating system.
        """
        try:
            rating_map = {
                13: 0.5, 1: 1.0, 54: 1.5, 64: 2.0, 118: 2.5, 
                128: 3.0, 186: 3.5, 196: 4.0, 242: 4.5, 255: 5.0
            }
            return rating_map[mutagen_mp3.tags.getall('POPM')[0].rating]
        except KeyError:
            return 0.0
    
    out['Rating'] = _mp3rating(file)     
    out['Time'] = file.info.length
    out['Bitrate'] = file.info.bitrate
    out['Extension'] = 'mp3'
    out['Filename'] = filepath.name
             
    return out
