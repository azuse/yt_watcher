from __future__ import unicode_literals
import yt_dlp as youtube_dl
import yt_local_db_manager as db
import json
import logging
import os
from pathlib import Path
from utils import file_exists, MyLogger
from config import VIDEO_FOLDER

def my_hook(d):
    if d['status'] == 'finished':
        print('Done downloading, now converting ...')

ydl_opts = {
    'cookiefile': 'cookies.txt',
    'skip_download': True,
    'format': 'bestaudio/best',
    'logger': MyLogger(),
    'progress_hooks': [my_hook],
    'ignoreerrors': True,
    'output': "videos",
    "outtmpl": "%(title)s-%(id)s.%(ext)s"
}
ydl = youtube_dl.YoutubeDL(ydl_opts)

ydl_downloader_opts = ydl_opts
ydl_downloader_opts["skip_download"] = False
ydl_downloader = youtube_dl.YoutubeDL(ydl_downloader_opts)

def add_playlist_to_db(url):
    # r = ydl.download([URL])
    r = ydl.extract_info(url, download=False)
    videos = r.get("entries", [])
    playlist_name = r.get("title", "")
    playlist_code = r.get("id", "")
    playlist_uploader = r.get("uploader", "")
    playlist_url = r.get("webpage_url", "")

    db.insert_playlist(playlist_name, playlist_code, playlist_url, playlist_uploader)

    for idx, video in enumerate(videos):
        print("Processing video: {}/{}".format(idx, len(videos)))
        if video is None:
            continue
        video_name = video.get("title", "")
        video_code = video.get("id", "")
        video_url = video.get("webpage_url", "")
        uploader = video.get("uploader", "")
        uploadtime = video.get("upload_date", "")
        file_path = "NOT_DOWNLOADED"
        file_size = video.get("filesize", "")

        db.insert_video(video_name, video_code, video_url, uploader, uploadtime, file_path, file_size, playlist_name, playlist_code)
    
    logging.info("Play list added! {}".format(playlist_name))

def extract_video_file_info(video_url):
    r = ydl.extract_info(video_url)
    filename = ydl.prepare_filename(r)
    return filename

def copy_video_folder(file_name):
    if not file_exists(file_name):
        return
    Path(VIDEO_FOLDER).mkdir(parents=True, exist_ok=True)
    os.rename(file_name, "{}/{}".format(VIDEO_FOLDER, file_name))
    
def update_video_in_db(title, id, file_path, file_size, file_status):
    pass


def download_video(video_url):
    
    file_name = extract_video_file_info(video_url)

    r = ydl_downloader.download([video_url])

    if not file_exists(file_name):
        logging.error("Download fail")
        return

    logging.info("download success")
    copy_video_folder(file_name)
    
    file_path = "{}/{}".format(VIDEO_FOLDER, file_name)
    file_status = "DOWNLOAED"
    
    db.update_video(video_url=video_url, file_path=file_path, file_status=file_status)
        
    
def sync_playlist(playlist_name=None, playlist_url=None, skip_download=True):
    if playlist_name is None and playlist_url is None:
        logging.error("playlist name and url not provided")
        return False
    
    if playlist_name is not None:
        videos = db.select_videos_by_playlist_name(playlist_name)
    else:
        videos = db.select_videos_by_playlist_url(playlist_url)
    
    for video in videos:
        video_url = video["video_url"]
        file_status = video["file_status"]
        if file_status is not None and skip_download:
            continue
        download_info = download_video(video_url)
        # update_video_info()

def sync_all_playlists():
    playlists = db.select_all_playlists()
    for playlist in playlists:
        sync_playlist(playlist["playlist_name"], playlist["playlist_url"])
    logging.info("Sync finished")


