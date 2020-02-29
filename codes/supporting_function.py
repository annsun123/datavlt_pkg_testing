# -*- coding: utf-8 -*-
"""
Created on Wed Feb  5 20:55:47 2020

@author: anyan
"""

import psycopg2
import psycopg2.extras
import json
from codes.class_logging import logging_func

spt_function=logging_func('jsonfunction_log',filepath='')
jsonlogger = spt_function.myLogger()

#with open('./json_credential/postgres_credential_external.json') as file:
with open('./json_credential/postgres_credential_amazon.json') as file:
    data = json.load(file)
    
def create_conn():
    conn=None
    #conn = psycopg2.connect("host='test-db.cxxcau9jx15i.ap-southeast-1.rds.amazonaws.com' port=5432 dbname='testdb' user='scott' password='mkgi7Fq5MhLO80uf'") 
    conn = psycopg2.connect("host='"+data['db_host']+"' "+"port='"+data['db_port']+"' "+"dbname='"+data['db_name']+"' "+"user='"+data['db_user']+"' "+"password='"+data['db_password']+"'")    
    return conn


def creating_graph_json(topltn_df, graph_description, filter_link):
    
    json={}
    value=topltn_df.to_dict(orient='records')
    json['graph_description'] = graph_description                       
    json['filter_links']= filter_link
    json['output'] = value                        

    return json




def value_existance(conn, to_date, from_date, indo_type):
    cur = conn.cursor()

    cur.execute("select * from json_main where to_date=%s and from_date=%s and category=%s" ,
                (to_date, from_date, indo_type))
    status = bool(cur.rowcount)

    return status



def table_check(conn, command_exists):
    cur = conn.cursor()
    cur.execute(command_exists)
    rst = cur.fetchone()[0]
    cur.close()
    return rst 



