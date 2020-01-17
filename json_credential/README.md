This folder have two json files for now. One is overall_credential and another is postgres_credential_external.

overall_credential:

    * google_api contains geo_api credential which is require when calling longtitude and latitude information from google API
    * goolge_api also contains the google drive configuration information which is file_address and archieve address which are input of googleFiling.py
    * mailing_credential is for log mailing config
    * scheduler_time contains hour and minutes assigning to Apscheduler
    * file_delete_from_googledrive: can choose delete or not_delete
  1. google_api: {geo_api,file_address, archive_address}     
  2. mailing_credential {from_address, receiving_address, user_name, password}
  3. scheduler_time: {hour, minutes}
  4. file_delete_from google_drive {delete, not_delete}

postgres_credential_external:


  


