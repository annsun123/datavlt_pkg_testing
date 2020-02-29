# -*- coding: utf-8 -*-
"""
Created on Sun Feb  9 14:30:25 2020
@author: anyan.sun
"""

import logging
import psycopg2
import numpy as np
import pandas as pd
from codes.otherFunction import create_conn

from json_function.codes.json_table_main import json_graph_indo, json_graph_nonindo
from codes.otherFunction import create_conn
#API3
def json_api (to_date, from_date, category_type, date_range):
    '''
    to_date and from_date are dates selected by customervm
    expeted input date in str type  and format of '%Y-%M-%D'
    category_type: 'indo', 'non_indo'    
    '''   
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
            json_list = np.array(pd.read_sql_query("select json_file from json_main \
                                                   where category='rfm' and \
                                                   process_date = (select max(process_date) \
                                                   from json_main where category='rfm')",\
                                                                   conn)).tolist()
            cur.close()
            conn.close()
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
    