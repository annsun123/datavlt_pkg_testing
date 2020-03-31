# -*- coding: utf-8 -*-
"""
Created on Wed Feb  5 21:10:29 2020

@author: anyan
"""
from json_function.codes.supporting_function import create_conn, value_existance
from json_function.codes.json_generating import creating_json_nonindo
from json_function.codes.json_generating_indo import creating_json_indo 
from json_function.codes.json_gen_mobile import json_main_mobile
from codes.class_dbinsert import insert_json_table
from psycopg2.extras import Json
import pandas as pd
import datetime
from json_function.codes.logging import logging_func

json_function=logging_func('jsontable_log',filepath='/')
jtablelogger = json_function.myLogger()
    
def json_graph_nonindo(to_date, from_date, date_range):
    #call the data from the database
    conn=create_conn()
    cur = conn.cursor()
    '''
    this function will only be used for REST api
    The inital json_file inserting happens after processing uploaded table
    
    '''
    
    if to_date == '' and from_date == '':
        
        json_output = pd.read_sql_query( "select json_file from json_main  where to_date = '' \
                     and category='non_indo'\
                     and dt_range='" + date_range + "'\
                    and process_date=(select max(process_date) from json_main where category='non_indo' \
                    and dt_range='" + date_range + "')", conn)
        
        json_list=[x for x in [json_output.iloc[i][0] for i in range(len(json_output))]]
        jtablelogger.info('retriving past six month non-indo data')
        cur.close()
        conn.close()
    else: 
        status = value_existance(conn,'json_main', to_date, from_date, 'non_indo')
        
        if status:
            json_output = pd.read_sql_query( "select json_file from json_main  where to_date = '" + \
                to_date + "' and from_date = '"+ from_date +\
                "' and category='non_indo'  ", conn)
                #and process_date = (select max(process_date) from json_main)
            json_list=[x for x in [json_output.iloc[i][0] for i in range(len(json_output))]]    
            cur.close()
            conn.close()
            jtablelogger.info('json_file of non-indo already exists')

        else:
            jtablelogger.info('json_file of non-indo _does not exists')
            final_table_nonindo = pd.read_sql_query("SELECT * from final_nonindo where invoice_date >= '" +\
            from_date + "' and invoice_date <= '" + to_date + "'", conn)
   
            cur.close()
            conn.close()
    
            # there are 14 pieces json for graphs in the json_list    
            json_list = creating_json_nonindo(final_table_nonindo)
            db_json_table=[]
            for i in json_list:
                db_json_table.append([i['graph_description'], 'non_indo', 'custom_date',\
                                     to_date, from_date, datetime.date.today(), Json(i)])
            
    
            insert_json_table(db_json_table, 'json_main')
            jtablelogger.info('json files insert successfully into the table')
            
    return json_list  
    
    
def json_graph_indo(to_date, from_date,date_range):
    #call the data from the database
    conn=create_conn()
    cur = conn.cursor()

    if to_date == '' and from_date == '':
        jtablelogger.info('retriving past six month indo data')
       
        json_output = pd.read_sql_query( "select json_file from json_main  where to_date = '' \
                     and category='indo'\
                     and dt_range='" + date_range + "'\
                    and process_date=(select max(process_date) from json_main where category='indo' \
                    and dt_range='" + date_range + "')", conn)
        json_list=[x for x in [json_output.iloc[i][0] for i in range(len(json_output))]]
 
        cur.close()
        conn.close()
    else: 
        
        status = value_existance(conn, 'json_main', to_date, from_date, 'indo')
        if status:
              json_output = pd.read_sql_query( "select json_file from json_main  where to_date = '" + \
                to_date + "' and from_date = '"+ from_date +\
                "' and category='indo' ", conn)
                #and process_date = (select max(process_date) from json_main)
              json_list=[x for x in [json_output.iloc[i][0] for i in range(len(json_output))]]  
              cur.close()
              conn.close()
              jtablelogger.info('json_file of indo already exists')


        else:
            jtablelogger.info('json_file of indo does not exists')
            final_table_indo = pd.read_sql_query("SELECT * from final_indo where invoice_date >= '" +  \
            from_date + "' and invoice_date <= '" + to_date + "'", conn)
   
            cur.close()
            conn.close()
    
            # there are 13 pieces json for graphs in the json_list    
            json_list = creating_json_indo(final_table_indo)

            db_json_table=[]
            for i in json_list:
                   
                db_json_table.append([i['graph_description'], 'indo', 'custom_date', \
                                     to_date, from_date, datetime.date.today(), Json(i)])
            insert_json_table(db_json_table,'json_main')
        
    return json_list     



def json_mobile(to_date,from_date,date_range,category_type):
    #call the data from the database
    conn=create_conn()
    cur = conn.cursor()

    if to_date == '' and from_date == '':
        jtablelogger.info('retriving past six month indo data')
       
        json_output = pd.read_sql_query( "select json_file from json_mobile  where to_date = '' \
                     and category = '" + category_type + \
                     "' and dt_range='" + date_range + "'\
                    and process_date=(select max(process_date) from json_main where category='" +  category_type+ \
                    "' and dt_range='" + date_range + "')", conn)
        json_list=[x for x in [json_output.iloc[i][0] for i in range(len(json_output))]]
 
        cur.close()
        conn.close()
    else: 
        
        status = value_existance(conn,'json_mobile', to_date, from_date, category_type)
        if status:
              json_output = pd.read_sql_query( "select json_file from json_mobile  where to_date = '" + \
                to_date + "' and from_date = '"+ from_date +\
                "' and category ='" + category_type+ "'", conn)
                #and process_date = (select max(process_date) from json_main)
              json_list=[x for x in [json_output.iloc[i][0] for i in range(len(json_output))]]  
              cur.close()
              conn.close()
              jtablelogger.info('json_file of indo already exists')


        else:
            jtablelogger.info('json_file of indo does not exists')
            final_table_ = pd.read_sql_query("SELECT * from final_"+ category_type.replace('_', '') +" where invoice_date >= '" +  \
            from_date + "' and invoice_date <= '" + to_date + "'", conn)
   
            cur.close()
            conn.close()
    
            # there are 13 pieces json for graphs in the json_list    
            json_list = json_main_mobile(final_table_, category_type)

            db_json_table=[]
            for i in json_list:
                   
                db_json_table.append([i['graph_description'], category_type, 'custom_date', \
                                     to_date, from_date, datetime.date.today(), Json(i)])
            insert_json_table(db_json_table, 'json_mobile')
        
    return json_list     
