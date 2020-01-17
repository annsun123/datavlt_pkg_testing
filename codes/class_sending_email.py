# -*- coding: utf-8 -*-
"""
Created on Mon Dec  9 09:47:51 2019

@author: anyan.sun
"""
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import encoders
from email.mime.base import MIMEBase
import datetime
import os 


def sending_emails(from_address,user_name,password,receiving_address):
    EMAIL_HOST = 'email-smtp.us-east-1.amazonaws.com'
    EMAIL_HOST_USER = user_name#"AKIAWHILWELFJY4H7MG2" # Replace with your SMTP username
    EMAIL_HOST_PASSWORD =password #"BGjsPmXs4QH3yzaeX9dmheu0KdTnWKKOnmx5D4+NjuSd" # Replace with your SMTP password
    EMAIL_PORT = 587
    
     
    
    msg = MIMEMultipart('alternative')
    msg['Subject'] = "Error_logs "
    msg['From'] = from_address # "noreply@datavlt.com"
    msg['To'] =",".join(receiving_address) # "anyan.sun@datavlt.com"
    
     
    #### First Half######
    email_content='This email contains log files of '+format(datetime.datetime.now(),"%m_%d_%Y")
    mime_text = MIMEText(email_content, 'html')
    msg.attach(mime_text)
    
     
    ######### Second Half ############
    for file in os.listdir('log'):
        attachment=open('log/'+file,'rb')
        file_name=file
        part = MIMEBase('application', "octet-stream")
        part.set_payload(attachment.read())  #if wnat to sending the log fi
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment', filename=file_name)
        msg.attach(part)
    
     
    ############# Sending the emial ##########
    s = smtplib.SMTP(EMAIL_HOST, EMAIL_PORT)
    s.starttls()
    s.login(EMAIL_HOST_USER, EMAIL_HOST_PASSWORD)
    s.sendmail(from_address, receiving_address, msg.as_string())
    s.quit()
    
    