# -*- coding: utf-8 -*-
"""
Created on Tue Mar 24 11:44:18 2020

@author: anyan.sun
"""


import pandas as pd 
from codes.otherFunction import create_conn


def mobile_api (category):
    
    '''
    if category_type=='insight', 'other', 'indomaret'
    '''
    
    try: 
       
        conn=create_conn()
        cur = conn.cursor()
        
        json_output = pd.read_sql_query( "select json_file from json_mobile  where category='" + category + \
                                        "' and process_date = (select max(process_date) \
                                               from json_mobile where category='" + category + "')",\
                                                               conn)
            #and process_date = (select max(process_date) from json_main)
        json_list=[x for x in [json_output.iloc[i][0] for i in range(len(json_output))]]  
        
        cur.close()
        conn.close()

        return json_list
    
    except Exception as e:
        
        logging.error('error happening within json_graph_xx functions')
        # for testing only
        return ({'to_date':to_date,'from_date':from_date,'category':category_type})
 




