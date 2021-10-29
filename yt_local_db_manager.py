import sqlite3
import os
import logging
from utils import file_exists

db = None

def create_default_table():
    SQL = """CREATE TABLE IF NOT EXISTS user_video 
            (id INTEGER PRIMARY KEY AUTOINCREMENT, 
            video_name TEXT, 
            video_code CHAR(100) UNIQUE,
            video_url TEXT, 
            uploader TEXT, 
            uploadtime DATETIME,
            file_path TEXT, 
            file_size TEXT,
            file_status TEXT,
            playlist_name TEXT,
            playlist_code TEXT);
        """
    logging.debug(SQL)
    db.execute(SQL)
    db.commit()

    SQL = """
    CREATE TABLE IF NOT EXISTS user_playlist
    (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        playlist_name TEXT UNIQUE,
        playlist_code CAHR(100),
        playlist_url TEXT,
        uploader TEXT
    )
    """
    logging.debug(SQL)
    db.execute(SQL)
    db.commit()


def truncate_table(name):
    SQL = "DELETE FROM {}".format(name)
    db.execute(SQL)
    db.commit()
    
def escape_sqlite(keyWord):
    keyWord = keyWord.replace("/", "//")
    keyWord = keyWord.replace("'", "''")
    keyWord = keyWord.replace("[", "/[")
    keyWord = keyWord.replace("]", "/]")
    keyWord = keyWord.replace("%", "/%")
    keyWord = keyWord.replace("&","/&")
    keyWord = keyWord.replace("_", "/_")
    keyWord = keyWord.replace("(", "/(")
    keyWord = keyWord.replace(")", "/)")
    return keyWord

def anti_escape_sqlite(keyWord):
    keyWord = keyWord.replace("//", "/")
    keyWord = keyWord.replace("''", "'")
    keyWord = keyWord.replace("/[", "[")
    keyWord = keyWord.replace("/]", "]")
    keyWord = keyWord.replace("/%", "%")
    keyWord = keyWord.replace("/&","&")
    keyWord = keyWord.replace("/_", "_")
    keyWord = keyWord.replace("/(", "(")
    keyWord = keyWord.replace("/)", ")")
    return keyWord

#                 INSERT              #
def insert_video(video_name, video_code, video_url, uploader, uploadtime, file_path, file_size, playlist_name, playlist_code):
    video_name = escape_sqlite(video_name)
    uploader = escape_sqlite(uploader)
    playlist_name = escape_sqlite(playlist_name)
    
    SQL = """INSERT INTO user_video (video_name, video_code, video_url, uploader, uploadtime, file_path, file_size, playlist_name, playlist_code) VALUES ('{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}');"""\
    .format(video_name, video_code, video_url, uploader, uploadtime, file_path, file_size, playlist_name, playlist_code)
    logging.debug(SQL)
    db.execute(SQL)
    db.commit()   

def insert_playlist(playlist_name, playlist_code, playlist_url, uploader):
    playlist_name = escape_sqlite(playlist_name)
    uploader = escape_sqlite(uploader)

    
    SQL = """
    INSERT INTO user_playlist 
    (playlist_name, playlist_code, playlist_url, uploader)
    VALUES
    ('{}', '{}', '{}', '{}')
    """.format(playlist_name, playlist_code, playlist_url, uploader)
    logging.debug(SQL)
    db.execute(SQL)
    db.commit()

#                 UPDATE              #
def update_video(video_name=None, video_code=None, video_url=None, uploader=None, uploadtime=None, file_path=None, file_size=None, playlist_name=None, playlist_code=None):
    if video_name is None and video_code is None:
        logging.error("video_name or code not provided")
        return

    video_old = select_video(video_name, video_code)
    video_name = video_name or video_old.get("video_name")
    video_code = video_code or video_old.get("video_old")
    video_url = video_url or video_old.get("video_url")

    video_name = escape_sqlite(video_name)
    uploader = escape_sqlite(uploader)
    

    SQL = """INSERT INTO user_video (video_name, video_code, video_url, uploader, uploadtime, file_path, file_size, playlist_name, playlist_code) VALUES ('{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}');"""\
    .format(video_name, video_code, video_url, uploader, uploadtime, file_path, file_size, playlist_name, playlist_code)
    logging.debug(SQL)
    db.execute(SQL)
    db.commit()   

#                 SELECT              #
def select_videos_by_playlist_name(name):
    name = escape_sqlite(name)

    SQL = """
    SELECT video_name, video_url, file_path, file_status
    FROM user_video
    WHERE playlist_name='{}';
    """.format(name)
    logging.debug(SQL)
    rows = db.execute(SQL)

    videos = []
    for row in rows:
        logging.info(row)
        video = {}
        video["video_name"] = anti_escape_sqlite(row[0])
        video["video_url"] = row[1]
        video["file_path"] = row[2]
        video["file_status"] = row[3]
        videos.append(video)

    return videos

def select_videos_by_playlist_url(url):
    SQL = """
    SELECT video_name, video_url, file_path, file_status
    FROM user_video JOIN user_playlist ON user_video.playlist_name=user_playlist.playlist_name
    WHERE user_playlist.playlist_url='{}';
    """.format(url)
    logging.debug(SQL)
    rows = db.execute(SQL)

    videos = []
    for row in rows:
        logging.info(row)
        video = {}
        video["video_name"] = anti_escape_sqlite(row[0])
        video["video_url"] = row[1]
        video["file_path"] = row[2]
        video["file_status"] = row[3]
        videos.append(video)

    return videos

def select_all_playlists():
    SQL = """
    SELECT playlist_name, playlist_code, playlist_url, uploader
    FROM user_playlist;
    """
    rows = db.execute(SQL)

    playlists = []
    for row in rows:
        logging.info(row)
        playlist = {
            "playlist_name": anti_escape_sqlite(row[0]),
            "playlist_code": row[1],
            "playlist_url": row[2],
            "uploader": anti_escape_sqlite(row[3])
        }
        playlists.append(playlist)
    
    return playlists

def select_video(video_name=None, video_code=None):
    SQL = """SELECT video_name, video_code, video_url, uploader, uploadtime, file_path, file_size, file_status, playlist_name, playlist_code FROM user_video """
    if video_name is not None:
        video_name = escape_sqlite(video_name)
        SQL = "{} WHERE video_name = '{}';".format(SQL, video_name)
    elif video_code is not None:
        SQL = "{} WHERE video_code = '{}';".format(SQL, video_code)
    logging.debug(SQL)
    rows = db.execute(SQL)

    video = {}
    for row in rows:
        video["video_name"] = anti_escape_sqlite(row[0])
        video["video_code"] = row[1]
        video["video_url"] = row[2]
        video["uploader"] = anti_escape_sqlite(row[3])
        video["uploadtime"] = row[4]
        video["file_path"] = row[5]
        video["file_size"] = row[6]
        video["file_status"] = row[7]
        video["playlist_name"] = anti_escape_sqlite(row[8])
        video["playlist_code"] = row[9]
        return video
    return None

DB_FILENAME = "youtube_data.sqlite"
if file_exists(DB_FILENAME):
    db = sqlite3.connect(DB_FILENAME)
    create_default_table()
else:
    db = sqlite3.connect(DB_FILENAME)
    create_default_table()

