# -*- coding: utf-8 -*-
"""
Created on Sun Feb  9 14:30:25 2020
@author: anyan.sun
"""

import logging
import pandas as pd
from codes.otherFunction import create_conn
from json_function.codes.json_table_main import json_graph_indo, json_graph_nonindo, json_mobile

#API3
def json_api (to_date, from_date, category_type, date_range, web):
    
    '''
    to_date and from_date are dates selected by customervm
    expeted input date in str type  and format of '%Y-%M-%D'
    category_type: 'indo', 'non_indo', 'rfm' 
   
    '''
    if(web == True):
        try:
            if type(to_date) != str:
                to_date = to_date.strftime('%Y-%m-%d')
                from_date = from_date.strftime('%Y-%m-%d')
            if category_type.lower() == 'indo':
                json_list = json_graph_indo(to_date, from_date, date_range)
            elif category_type.lower() == 'non_indo': 
                json_list = json_graph_nonindo(to_date, from_date, date_range)
            elif category_type.lower() == 'rfm':
                conn = create_conn()
                cur = conn.cursor()
                json_output  = pd.read_sql_query("select json_file from json_main \
                                                       where category='rfm' and \
                                                       process_date = (select max(process_date) \
                                                       from json_main where category='rfm')",\
                                                                       conn)
                cur.close()
                conn.close()
                json_list=[x for x in [json_output.iloc[i][0] for i in range(len(json_output))]]  
            else:
                return ({'error': 'non_existing_table'})
            
            return json_list
        except Exception as e:
            logging.error('error happening within json_graph_xx functions')
            # for testing only
            return ({'to_date':to_date,'from_date':from_date,'category':category_type})
        
    if(web == False):
        try:
            if type(to_date) != str:
                to_date = to_date.strftime('%Y-%m-%d')
                from_date = from_date.strftime('%Y-%m-%d')
            if category_type.lower() != 'rfm':
                json_list = json_mobile(to_date, from_date, date_range, category_type)

            elif category_type.lower() == 'rfm':
                conn = create_conn()
                cur = conn.cursor()
                json_output  = pd.read_sql_query("select json_file from json_mobile \
                                                       where category='rfm' and \
                                                       process_date = (select max(process_date) \
                                                       from json_mobile where category='rfm')",\
                                                                       conn)
                cur.close()
                conn.close()
                json_list=[x for x in [json_output.iloc[i][0] for i in range(len(json_output))]]  
            else:
                return ({'error': 'non_existing_table'})
            
            return json_list
        except Exception as e:
            logging.error('error happening within json_graph_xx functions')
            # for testing only
            return ({'to_date':to_date,'from_date':from_date,'category':category_type})

#API2
# def status_api():
#     """
#     query from postgres's status table to get status and timestamp
#     """
    