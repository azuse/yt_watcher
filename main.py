from __future__ import unicode_literals
import youtube_dl
import yt_local_db_manager as db
import json
import logging
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
logging.basicConfig(level=logging.DEBUG,
                    format='%(levelname)s: %(asctime)s - %(filename)s[line:%(lineno)d] - %(message)s')
import os
from pathlib import Path

from yt_watcher import *

URL = 'https://youtube.com/playlist?list=PL_HmVyhcPK5iBS6YCL9aEZDsRXkho-kJ1'

if __name__ == "__main__":
    add_playlist_to_db(URL)
    sync_all_playlists()