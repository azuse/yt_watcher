import sqlite3
import os
import logging
from utils import file_exists
from functools import wraps

db = None

def db_exception(function):
    @wraps(function)
    def exception_catcher(*args, **kwargs):
        try:
            result = function(*args, **kwargs)
            return result
        except Exception as e:
            logging.exception("DB EXECUTE EXCEPTION: %s", e)
    return exception_catcher

@db_exception
def create_default_table():
    SQL = """CREATE TABLE IF NOT EXISTS user_video 
            (id INTEGER PRIMARY KEY AUTOINCREMENT, 
            video_name TEXT UNIQUE, 
            video_code CHAR(100) UNIQUE,
            video_url TEXT UNIQUE, 
            uploader TEXT, 
            uploadtime DATETIME,
            file_path TEXT, 
            file_size TEXT,
            file_status TEXT,
            playlist_name TEXT,
            playlist_code TEXT);
        """
    logging.info(SQL)
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
    logging.info(SQL)
    db.execute(SQL)
    db.commit()

@db_exception
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
@db_exception
def insert_video(video_name, video_code, video_url, uploader, uploadtime, file_path, file_size, playlist_name, playlist_code):
    video_name = escape_sqlite(video_name)
    uploader = escape_sqlite(uploader)
    playlist_name = escape_sqlite(playlist_name)
    
    SQL = """INSERT INTO user_video (video_name, video_code, video_url, uploader, uploadtime, file_path, file_size, playlist_name, playlist_code) VALUES ('{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}');"""\
    .format(video_name, video_code, video_url, uploader, uploadtime, file_path, file_size, playlist_name, playlist_code)
    logging.info(SQL)
    db.execute(SQL)
    db.commit()   

@db_exception
def insert_playlist(playlist_name, playlist_code, playlist_url, uploader):
    playlist_name = escape_sqlite(playlist_name)
    uploader = escape_sqlite(uploader)

    
    SQL = """
    INSERT INTO user_playlist 
    (playlist_name, playlist_code, playlist_url, uploader)
    VALUES
    ('{}', '{}', '{}', '{}')
    """.format(playlist_name, playlist_code, playlist_url, uploader)
    logging.info(SQL)
    db.execute(SQL)
    db.commit()

#                 UPDATE              #
@db_exception
def update_video(video_name=None, video_code=None, video_url=None, uploader=None, uploadtime=None, file_path=None, file_size=None, file_status=None, playlist_name=None, playlist_code=None):
    if video_name is None and video_code is None and video_url is None:
        logging.error("video_name or code not provided")
        return

    video_old = select_video(video_name, video_code, video_url)
    video_name = video_name or video_old.get("video_name")
    video_code = video_code or video_old.get("video_code")
    video_url = video_url or video_old.get("video_url")
    uploader = uploader or video_old.get("uploader")
    uploadtime = uploadtime or video_old.get("uploadtime")
    file_path = file_path or video_old.get("file_path")
    file_size = file_size or video_old.get("file_size")
    file_status = file_status or video_old.get("file_status")
    playlist_name = playlist_name or video_old.get("playlist_name")
    playlist_code = playlist_code or video_old.get("playlist_code")

    video_name = escape_sqlite(video_name)
    uploader = escape_sqlite(uploader)
    

    SQL = """UPDATE user_video 
    SET video_name = '{}',
    video_code = '{}',
    video_url = '{}',
    uploader = '{}',
    uploadtime = {},
    file_path = '{}',
    file_size = '{}',
    file_status = '{}',
    playlist_name = '{}',
    playlist_code = '{}'
    WHERE video_code = '{}';
    """\
    .format(video_name, video_code, video_url, uploader, uploadtime, file_path, file_size, file_status, playlist_name, playlist_code, video_code)
    logging.info(SQL)
    db.execute(SQL)
    db.commit()   

#                 SELECT              #
@db_exception
def select_videos_by_playlist_name(name):
    name = escape_sqlite(name)

    SQL = """
    SELECT video_name, video_url, file_path, file_status
    FROM user_video
    WHERE playlist_name='{}';
    """.format(name)
    logging.info(SQL)
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

@db_exception
def select_videos_by_playlist_url(url):
    SQL = """
    SELECT video_name, video_url, file_path, file_status
    FROM user_video JOIN user_playlist ON user_video.playlist_name=user_playlist.playlist_name
    WHERE user_playlist.playlist_url='{}';
    """.format(url)
    logging.info(SQL)
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

@db_exception
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

@db_exception
def select_video(video_name=None, video_code=None, video_url=None):
    SQL = """SELECT video_name, video_code, video_url, uploader, uploadtime, file_path, file_size, file_status, playlist_name, playlist_code FROM user_video """
    if video_name is not None:
        video_name = escape_sqlite(video_name)
        SQL = "{} WHERE video_name = '{}';".format(SQL, video_name)
    elif video_code is not None:
        SQL = "{} WHERE video_code = '{}';".format(SQL, video_code)
    elif video_url is not None:
        SQL = "{} WHERE video_url = '{}';".format(SQL, video_url)
    else:
        return {}
        
    logging.info(SQL)
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

