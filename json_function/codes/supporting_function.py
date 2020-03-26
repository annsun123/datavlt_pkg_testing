# -*- coding: utf-8 -*-
"""
Created on Wed Feb  5 20:55:47 2020

@author: anyan
"""
 
import psycopg2
import psycopg2.extras
import json
from json_function.codes.logging import logging_func
import datetime
import time

spt_function=logging_func('jsonfunction_log',filepath='/')
jsonlogger = spt_function.myLogger()

with open('./json_credential/postgres_credential_amazon.json') as file:
#with open('./json_function/credentials/postgres_credential_docker.json') as file:
    data=json.load(file)

def getDateRangeFromWeek(p_year,p_week):
    p_week=p_week-1
    firstdayofweek = datetime.datetime.strptime('{}-W{}-1'.format(p_year,p_week), "%Y-W%W-%w").date()
    lastdayofweek = firstdayofweek + datetime.timedelta(days=6.9)
    return firstdayofweek, lastdayofweek

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



def insert_json_table(values_list, table_name, table_command):

    try:
        command_create = (table_command)

        command_exists = (
            """
            SELECT EXISTS (
            SELECT 1
            FROM  information_schema.tables
            WHERE table_schema = 'public'
            AND table_name ='""" + table_name+ "');"
        )

        con = create_conn()
        cur = con.cursor()
        rst = table_check(con, command_exists)
        if (rst):
            jsonlogger.info('table exists')
            cur = con.cursor()
            psycopg2.extras.execute_values(cur, 'INSERT INTO '+ table_name + ' VALUES %s', values_list)
            con.commit()
            if(cur.rowcount > 0):
                jsonlogger.info('Rows Inserted Sucessfully in table')
            else:
                jsonlogger.info('Could not insert Rows in table')
            cur.close()
        else:
            jsonlogger.info('create table')
            cur = con.cursor()
            cur.execute(command_create)
           # try:
            psycopg2.extras.execute_values(cur, 'INSERT INTO '+ table_name + ' VALUES %s', values_list)
            con.commit()
            if(cur.rowcount > 0):
                jsonlogger.info('Rows Inserted Sucessfully in table')
            else:
                jsonlogger.info('Could not insert Rows in table')
            cur.close()
    except Exception as e:
        con.rollback()
        jsonlogger.error(e)
    finally:
        con.commit()
        cur.close()
        con.close()

def color_variant(hex_color, brightness_offset=1):
    """ takes a color like #87c95f and produces a lighter or darker variant """
    if len(hex_color) != 7:
        raise Exception("Passed %s into color_variant(), needs to be in #87c95f format." % hex_color)
    rgb_hex = [hex_color[x:x+2] for x in [1, 3, 5]]
    new_rgb_int = [int(hex_value, 16) + brightness_offset for hex_value in rgb_hex]
    new_rgb_int = [min([255, max([0, i])]) for i in new_rgb_int] # make sure new values are between 0 and 255
    # hex() produces "0x88", we want just "88"
    return "#" + "".join([hex(i)[2:] for i in new_rgb_int])
