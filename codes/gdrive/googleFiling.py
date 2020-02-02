# -*- coding: utf-8 -*-
"""
Created on Fri Nov 29 11:35:03 2019

@author: anyan.sun
"""

import datetime

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from codes.class_logging import logging_func
logger = logging_func('gdrive_log', filepath='')
dowfilelogger = logger.myLogger()


def downloadfiles(downloadPath, google_api, delete_status):

    today_dt = datetime.date.today().strftime("%Y-%m-%d")
    cnt = 0

   # Get connection
    gauth = GoogleAuth('./codes/gdrive/settings.yaml')

    gauth.LoadCredentialsFile('./codes/gdrive/credentials.json')
    drive = GoogleDrive(gauth)

   # get files from main folder and folder id of Archive folder
   # fileList = drive.ListFile({'q': "'19nFd8wCUMsjVqrbEQpfdBaAjj2ja-31g' in parents and trashed=false"}).GetList()
    fileList = drive.ListFile({'q': "'" + google_api['file_address'] + "' in parents and trashed=false"}).GetList()

    file_list_archive = google_api['archive_address']

    file_downloaded = []
    for file in fileList:
        if 'sheet' in file.metadata['mimeType'] or 'ms-excel' in file.metadata['mimeType']:
            cnt += 1

            dowfilelogger.info('Downloading file %s from Google Drive' % file['title'])
            file.GetContentFile(downloadPath + file['title'])  # Save Drive file as a local file

            dowfilelogger.info('Archving file')
            file_arch = drive.CreateFile({"parents": [{"kind": "drive#fileLink", "id": file_list_archive}]})
            file_arch.SetContentFile(downloadPath + file['title'])
            dowfilelogger.info('successfully download')
# renaming file to archive
            filename_arch = file['title'].split('.')
            file_downloaded.append(file['title'])
            file_arch['title'] = filename_arch[0] + "_" + today_dt + '.' + filename_arch[1]
            file_arch.Upload()
            if delete_status == "T":

                dowfilelogger.info('Deleting file from folder')
                file.Delete()

    dowfilelogger.info('number of files', cnt)
    return file_downloaded, cnt
