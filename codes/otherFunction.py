# -*- coding: utf-8 -*-
"""
Created on Fri Nov 29 11:17:55 2019

@author: anyan.sun

"""

import pandas as pd
import requests
from time import sleep
import psycopg2
import psycopg2.extras
from psycopg2.extras import Json
import datetime
from datetime import timedelta
import json
import logging
from codes.class_logging import logging_func

geo = logging_func('otherfunction_log', filepath='')
otherlogger = geo.myLogger()

# with open('./json_credential/postgres_credential_external.json') as file:
with open('./json_credential/postgres_credential_amazon.json') as file:
    data = json.load(file)


def google_geocode1(address_list, address_col, reference_col, cred):
    url = 'https://maps.googleapis.com/maps/api/geocode/json'
    result = []

    LIMIT = 2500
    unused_limit = LIMIT
    period = datetime.timedelta(hours=24)
    limit_start_time = datetime.datetime.now()
    first_pass = True
    est_passes = len(address_list) / LIMIT
    error_address = []
    for idx, rows in address_list.iterrows():
        address = rows['full_address']
        customer_id=rows[reference_col]
        answer = {
            reference_col: customer_id,
            'full_address': address,
            'Formatted Address': None,
            'Latitude': None,
            'Longitude': None
        }

        params = {'address': address, 'key': cred}
        response = requests.get(url, params=params)

        if response.status_code == 400:
            result.append(answer)
            logging.info('address not geocodeable at item ' + str(idx) + ': ' + str(address))
            continue
        if response.status_code != 200:
            result.append(answer)
            logging.info('API unavailable at item ' + str(idx) + ': ' + str(address))
            logging.info(response)
            break
        body = json.loads(response.text)

        if len(body['results']) == 0:
            error_address.append(idx)
            for i in range(len(address.split(','))):
                address = ','.join(address.split(',')[i + 1:]).lstrip()
                params = {'address': address, 'key': cred}
                response = requests.get(url, params=params)
                body = json.loads(response.text)
                if len(body['results']) != 0:
                    break
        else:
            body = body

        try:
            first_match = body['results'][0]
            answer['Formatted Address'] = first_match['formatted_address']
            location = first_match['geometry']['location']
            answer['Latitude'] = location['lat']
            answer['Longitude'] = location['lng']
            result.append(answer)
            otherlogger.info('Item ' + str(idx) + ' geocoded.')
        except (IndexError, KeyError) as e:
            result.append(answer)
            otherlogger.error('No location match for item ' + str(idx) + ': ' + str(address)) 

        unused_limit -= 1
        if unused_limit < 1:
            elapsed_period = datetime.now() - limit_start_time
            est_time = datetime.now() + timedelta(days=(est_passes))
            remaining_period = period - elapsed_period
            remaining_seconds = remaining_period.total_seconds()
            if remaining_seconds > 0 and first_pass == True and est_passes > 1:
                otherlogger.info('Reached free limit for today in ' + str(elapsed_period // 3600) + ' hours, ' + str((elapsed_period % 3600) // 60) + ' min.')
                otherlogger.info('Estimated completion at ' + str(est_time.isoformat()[:19]) + ' local time.')

                sleep(remaining_seconds)
            unused_limit = 2500

    if str(type(address_list)) == "<class 'pandas.core.series.Series'>":
        result_df = pd.DataFrame(result, columns=[reference_col,address_col,
                                                  'Formatted Address', 'Latitude',
                                                  'Longitude'], index=address_list.index)
    else:
        result_df = pd.DataFrame(result,
                                 columns=[reference_col,address_col,
                                          'Formatted Address', 'Latitude', 'Longitude'])
    otherlogger.info('Completed geocoding ' + str(len(address_list)) + ' addresses.')
    return result_df, error_address


def table_existance(conn,table_name):
    cur = conn.cursor()

    cur.execute("select * from information_schema.tables where table_name=%s",
                (table_name,))
    status = bool(cur.rowcount)
    cur.close()
    conn.close()
    return status


def table_check(conn, command_exists):
    cur = conn.cursor()
    cur.execute(command_exists)
    rst = cur.fetchone()[0]
    cur.close()
    return rst 


def create_conn():
    conn = None
# conn = psycopg2.connect("host='test-db.cxxcau9jx15i.ap-southeast-1.rds.amazonaws.com'
# port=5432 dbname='testdb' user='scott' password='mkgi7Fq5MhLO80uf'") 
    conn = psycopg2.connect("host='" + data['db_host'] + "' " + "port='" + data['db_port'] + "' " + "dbname='" + data['db_name'] + "' " + "user='" + data['db_user']+"' "+"password='" + data['db_password'] + "'")    
    return conn


# Jsson File Function
def sql_insert(table_name, data_dict):
    '''
        The format shall be:
        INSERT INTO table_name Graph,  Processing_Date,  output_json)
        VALUES (%(Graph)s, %(Processing_Date)s, %(output_json)s );
    '''

    sql = '''
        INSERT INTO %s (%s)
        VALUES (%%(%s)s );
        ''' % (table_name, ', '.join(data_dict), ')s, %('.join(data_dict))

    return sql


def item_creating(json_input, graph_type):
    item = {
        'Type': graph_type,
        'Processing_Date': datetime.date.today(),  # .strftime(' %d, %b %Y'),
        'output_json': Json(json_input)
    }
    return item  


def creating_plot(table_name, item, table_creating_command):

    conn = create_conn()

# table_name = table_name
    sql = sql_insert(table_name, item)
    status = table_existance(conn, table_name)

    try:

        if status:
            print('table exists')
            conn=create_conn()
            cur=conn.cursor()
            cur.execute(sql, item)
            conn.commit()
            if(cur.rowcount > 0):
                otherlogger.info('Rows Inserted Sucessfully in  table')
            else:
                otherlogger.info('Could not insert Rows in  table')

            cur.close()
        else:
            print('create table')
            conn = create_conn()
            cur = conn.cursor()

            cur.execute('CREATE TABLE ' + table_name + ' ' + table_creating_command)
            cur.execute(sql, item)
            conn.commit()
            if(cur.rowcount > 0):
                otherlogger.info('Rows Inserted Sucessfully in table')
            else:
                otherlogger.info('Could not insert Rows in  table')
            cur.close()
            cur.close()

    except Exception as e:
        otherlogger.info('not successful')
        conn.rollback()
    finally:
        conn.commit()
        cur.close()
        conn.close()
