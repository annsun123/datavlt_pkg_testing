import pandas as pd
import numpy as np
import datetime
import psycopg2
import math
from psycopg2.extras import Json
# Self Build-in Packages
from codes.otherFunction import create_conn, table_check
from codes.class_logging import logging_func
db_logging = logging_func('db_inserting_log', filepath='')
dblogger = db_logging.myLogger()


class inserting_table:
    def __init__(self,
                 transaction_nonindo,
                 transaction_indo,
                 final_customer_table,
                 master_list_city,
                 master_list_province,
                 master_list_shoptype,
                 master_list_address,
                 master_list_contact,
                 dc_col):
        self.transaction_nonindo = transaction_nonindo
        self.transaction_indo = transaction_indo
        self.final_customer_table = final_customer_table
        self.master_list_city = master_list_city
        self.master_list_province = master_list_province
        self.master_list_shoptype = master_list_shoptype
        self.master_list_address = master_list_address
        self.master_list_contact = master_list_contact
        self.dc_col = dc_col

    def mapping_nonindo_customer(self):

        dblogger.info('creating columns for df_transaction table')

        dblogger.info('mapping customer info with transaction table')

        try:

    #preparing four tables: final_non_indo, customer_non_indo, final_indo, cusotmer_indo
            self.transaction_nonindo['customer_id'] = self.transaction_nonindo['customer_id'].apply(lambda x: str(x))

            customer_non_indo = self.final_customer_table[
                self.final_customer_table['indo_type'] == 'NON_INDO'
            ]

    # mapping none indomarte transaction table to the none indomrate
    # customer table by using the methods of left merging

            final_non_indo = self.transaction_nonindo.merge(
                customer_non_indo[['customer_id',
                                   'city',
                                   'province',
                                   'shop_type',
                                   'customer_address',
                                   'latitude',
                                   'longtitude']],
                how='left',
                on=['customer_id']
            )

            non_customer_info = [x for x in final_non_indo['customer_id'].unique()
                                 if x not in customer_non_indo['customer_id'].unique()]

            dblogger.info('successfully update customer info to transaction table')

            df_transaction = final_non_indo.copy()
            df_transaction['Date'] = pd.to_datetime(df_transaction['Date'])

            dblogger.info('creating month, date, year, system date')

            df_transaction['Date'] = pd.to_datetime(df_transaction['Date'])
            df_transaction['year'] = df_transaction['Date'].apply(lambda x: x.year)
            df_transaction['month'] = df_transaction['Date'].apply(lambda x: x.month)
            df_transaction['week'] = df_transaction['Date'].apply(lambda x: x.week)
            #df_transaction['week_of_month'] = df_transaction['Date'].\
            #apply(lambda x: np.nan if pd.isnull(x) else \
             #     int(math.ceil((x.day+x.replace(day=1).weekday())/7.0)) )
            
            df_transaction['system_date'] = datetime.date.today()
            df_transaction.loc[df_transaction['city'].isnull(), 'city'] = 'CITY NA'
            dblogger.info('mapping successfully')
            return df_transaction, non_customer_info
        except:
            dblogger.info('please checking the table values')

    def mapping_indo_customer(self):

        dblogger.info('creating columns for df_transaction table')

        dblogger.info('mapping customer info with transaction table')

        try:

    # mapping indomarte transaction table to the indomrate customer table by using
    # the methods of left merging by creating new columns in transaction tables and mapping 
            customer_indo = self.final_customer_table[self.final_customer_table['indo_type']=='INDO']

            self.transaction_indo[self.master_list_city] = np.nan
            self.transaction_indo[self.master_list_province] = np.nan
            self.transaction_indo[self.master_list_shoptype] = np.nan
            self.transaction_indo['latitude'] = np.nan
            self.transaction_indo['longtitude'] = np.nan

            non_customer_info = []

            for x in self.transaction_indo[self.dc_col].unique():
                if x in customer_indo['customer_name'].unique():

                    self.transaction_indo.loc[self.transaction_indo[self.dc_col] == x, 
                                              [self.master_list_city,
                                               self.master_list_province,
                                               self.master_list_shoptype,
                                               'latitude',
                                               'longtitude']] =\
                                              customer_indo[customer_indo['customer_name'] == x][['city',\
                                                           'province', 'shop_type', 'latitude', \
                                                           'longtitude']].iloc[-1].values
                else:

                    non_customer_info.append(x)

            dblogger.info('successfully update customer info to transaction table')

            df_transaction = self.transaction_indo.copy()
            df_transaction['Invoice Date'] = pd.to_datetime(df_transaction['Invoice Date'])

            dblogger.info('creating month, date, year, system date')

            df_transaction['Invoice Date'] = pd.to_datetime(df_transaction['Invoice Date'])
            df_transaction['year'] = df_transaction['Invoice Date'].apply(lambda x: x.year)
            df_transaction['month'] = df_transaction['Invoice Date'].apply(lambda x: x.month)
            df_transaction['week'] = df_transaction['Invoice Date'].apply(lambda x: x.week)
            #df_transaction['week_of_month'] = df_transaction['Invoice Date'].\
            #apply(lambda x: np.nan if pd.isnull(x) else \
             #     int(math.ceil((x.day+x.replace(day=1).weekday())/7.0)) )
            df_transaction['system_date'] = datetime.date.today()
            
            df_transaction.loc[df_transaction[self.master_list_city].isnull(),
                               self.master_list_city] = 'CITY NA'
            return df_transaction, non_customer_info

        except:
            dblogger.info('please checking the table values')

    def inserting_customer_table(self):

        try:
            command_create = (
                """
             CREATE TABLE customer_table(
             customer_id varchar NOT NULL,
             customer_name text NOT NULL,
             customer_address text ,
             formal_address text,
             city text,
             province text,
             shop_type text,
             contact_name text,
             longtitude numeric,
             latitude numeric,
             error_type text NOT NULL,
             Indo_Type text NOT NULL,
             updated_date date NOT NULL);

                 """)
            command_exists = (
                """
                SELECT EXISTS (
                        SELECT 1
                        FROM   information_schema.tables
                        WHERE  table_schema = 'public'
                        AND    table_name = 'customer_table'
                        );
                """)

            con = create_conn()
            cur = con.cursor()
            rst = table_check(con, command_exists)
            if(rst):
                dblogger.info('table exists')
                cur = con.cursor()

                if 'Unnamed: 0' in self.final_customer_table.columns:
                    self.final_customer_table = self.final_customer_table.drop('Unnamed: 0', 1)
                else:
                    pass
                values = self.final_customer_table.values.tolist()
                psycopg2.extras.execute_values(cur, 'INSERT INTO customer_table VALUES %s', values)
                con.commit()
                if(cur.rowcount > 0):
                    dblogger.info('Rows Inserted Sucessfully in customer table')
                else:
                    dblogger.info('Could not insert Rows in customer table')
                cur.close()
            else:
                cur = con.cursor()
                cur.execute(command_create)

                if 'Unnamed: 0' in self.final_customer_table.columns:
                    self.final_customer_table = self.final_customer_table.drop('Unnamed: 0', 1)
                else:
                    pass

                values = self.final_customer_table.values.tolist()
                psycopg2.extras.execute_values(cur, 'INSERT INTO customer_table VALUES %s', values)
                con.commit()
                if(cur.rowcount > 0):
                    dblogger.info('Rows Inserted Sucessfully in customer table')
                else:
                    dblogger.info('Could not insert Rows in customer table')
                cur.close()

        except Exception as e:
            con.rollback()
            dblogger.info(e)
        finally:
            con.commit()
            cur.close()
            con.close()


def insert_nonindo_table(df_final):

    try:
        command_create = ("""
         CREATE TABLE final_nonindo(
         sku text NOT NULL,
         series_num text NOT NULL,
         customer_id numeric,
         customer_name text NOT NULL,
         invoice_date date NOT NULL,
         qty_mc numeric,
         qty_packs numeric,
         amount numeric,
         amount_million numeric,
         city text,
         province text,
         shop_type text,
         customer_address text,
         latitude numeric,
         longtitude numeric,
         year numeric,
         month numeric,
         week_of_month numeric,
         system_date date NOT NULL

        );

         """)

        command_exists = (
            """
            SELECT EXISTS (
            SELECT 1
            FROM   information_schema.tables
            WHERE  table_schema = 'public'
            AND    table_name = 'final_nonindo'
            );
            """
        )

        con = create_conn()
        cur = con.cursor()
        rst = table_check(con, command_exists)
        if (rst):
            dblogger.info('table exists')
            cur = con.cursor()
            psycopg2.extras.execute_values(cur, 'INSERT INTO final_nonindo VALUES %s', df_final.values.tolist())
            con.commit()
            if(cur.rowcount > 0):
                dblogger.info('Rows Inserted Sucessfully in final_nonindo')
            else:
                dblogger.info('Could not insert Rows in final_nonindo')
            cur.close()
        else:
            dblogger.info('create table')
            cur = con.cursor()
            cur.execute(command_create)
            psycopg2.extras.execute_values(cur, 'INSERT INTO final_nonindo VALUES %s', df_final.values.tolist())
            con.commit()
            if(cur.rowcount > 0):
                dblogger.info('Rows Inserted Sucessfully in final_nonindo')
            else:
                dblogger.info('Could not insert Rows in final_nonindo')
            cur.close()
    except Exception as e:
        con.rollback()
        dblogger.error(e)
    finally:
        con.commit()
        cur.close()
        con.close()


def insert_indo_table(df_final):

    try:
        command_create = ("""
         CREATE TABLE final_indo(
         invoice_date date NOT NULL,
         customer_name text NOT NULL,
         qty_packs numeric,
         amount numeric,
         series_num text NOT NULL,
         sku text NOT NULL,
         
         qty_mc numeric,
         amount_million numeric,
         city text,
         province text,
         shop_type text,
         latitude numeric,
         longtitude numeric,
         year numeric,
         month numeric,
         week_of_month numeric,
         system_date date NOT NULL 
        );

         """)

        command_exists = (
            """
            SELECT EXISTS (
            SELECT 1
            FROM  information_schema.tables
            WHERE table_schema = 'public'
            AND table_name = 'final_indo'
            );
            """
        )

        con = create_conn()
        cur = con.cursor()
        rst = table_check(con, command_exists)
        if (rst):
            dblogger.info('table exists')
            cur = con.cursor()
            psycopg2.extras.execute_values(cur, 'INSERT INTO final_indo VALUES %s', df_final.values.tolist())
            con.commit()
            if(cur.rowcount > 0):
                dblogger.info('Rows Inserted Sucessfully in final_indo')
            else:
                dblogger.info('Could not insert Rows in final_indo')
            cur.close()
        else:
            dblogger.info('create table')
            cur = con.cursor()
            cur.execute(command_create)
            psycopg2.extras.execute_values(cur, 'INSERT INTO final_indo VALUES %s', df_final.values.tolist())
            con.commit()
            if(cur.rowcount > 0):
                dblogger.info('Rows Inserted Sucessfully in final_indo')
            else:
                dblogger.info('Could not insert Rows in final_indo')
            cur.close()
    except Exception as e:
        con.rollback()
        dblogger.error(e)
    finally:
        con.commit()
        cur.close()
        con.close()

def insert_json_table(values_list, table_name):

    try:
        command_create = ("CREATE TABLE " + table_name+ """(
         graph_type text NOT NULL,
         category text NOT NULL,
         dt_range text NOT NULL,
         to_date text NOT NULL,
         from_date text NOT NULL,
         process_date date NOT NULL,
         Json_file JSONB
        );

         """)

        command_exists = (
            """
            SELECT EXISTS (
            SELECT 1
            FROM  information_schema.tables
            WHERE table_schema = 'public'
            AND table_name = '""" + table_name+ "');"
            
          
        )

        con = create_conn()
        cur = con.cursor()
        rst = table_check(con, command_exists)
        if (rst):
            dblogger.info('table exists')
            cur = con.cursor()
            psycopg2.extras.execute_values(cur, "INSERT INTO " + table_name+ " VALUES %s", values_list)
            con.commit()
            if(cur.rowcount > 0):
                dblogger.info('Rows Inserted Sucessfully in table')
            else:
                dblogger.info('Could not insert Rows in table')
            cur.close()
        else:
            dblogger.info('create table')
            cur = con.cursor()
            cur.execute(command_create)
            psycopg2.extras.execute_values(cur, "INSERT INTO " + table_name+ " VALUES %s", values_list)
            con.commit()
            if(cur.rowcount > 0):
                dblogger.info('Rows Inserted Sucessfully in table')
            else:
                dblogger.info('Could not insert Rows in table')
            cur.close()
    except Exception as e:
        con.rollback()
        dblogger.error(e)
    finally:
        con.commit()
        cur.close()
        con.close()

