# -*- coding: utf-8 -*-
"""
Created on Sun Dec  1 20:07:05 2019

@author: anyan.sun
"""

'''
This file mainly contains functions for inserting tables into the database, as well a mapping customer table with transaction table to form final_table 
Table Included in functions:
	1. inserting_customer_table
	2. insert_main_table
	3. mapping_customer_information 

mapping_customer_information:
	




Function 1: mapping_customer_information[
	

	4. extracting  year, month, week_of month, system_date from the 'Date' columns

	5. if the city is na, then replaced with str("CITY NA")

	Values passed to __init__:
		df_transaction: output of processing_cl.processing_history_excepterrorV2 (processed transaction table)
		customer_table: the output of processing_cl.first_customer_table
		The following varaibles are passed at begining of main.py which are the columns name. If the column name is changed or not passed correctly, the code will break. [This might be one of reasons of code breaking]
		master_list_city: The city column name in the customer table.
		master_list_province: the province column in customertable 
		master_list_shoptype: shopetyep col in customer table
		master_list_address: address col in customer table ]


Function 2: inserting_customer_table [
	command_create=() is a table create command, table columns parameters and table name are passed. If table insert into database is changing, please come to here changing table name. 
	command_exists=() checking whether table_name='customer_table' exits in the database or not

	If the table exists in the database then just insert the values into the table. If table does not exists, then create the table with table name and columns in command_create(). Then insert.


Function 2: inserting_main_table [
	same logic as the inserting_customer_table



'''
#### outside packages 
import pandas as pd
import numpy as np
import datetime
import psycopg2
import math
from psycopg2.extras import Json

#### Self Build-in Packages 
from codes.otherFunction import table_existance,create_conn,table_check
from codes.class_logging import logging_func


db_logging=logging_func('db_inserting_log',filepath='')
dblogger=db_logging.myLogger()


class inserting_table:
    def __init__(self,transaction_nonindo,transaction_indo,final_customer_table,master_list_city,master_list_province,master_list_shoptype,master_list_address,master_list_contact):
            self.transaction_nonindo=transaction_nonindo
            self.transaction_indo=transaction_indo
            self.final_customer_table=final_customer_table
            self.master_list_city=master_list_city
            self.master_list_province=master_list_province
            self.master_list_shoptype=master_list_shoptype
            self.master_list_address=master_list_address
            self.master_list_contact=master_list_contact
            
    def mapping_nonindo_customer(self):


        dblogger.info('creating columns for df_transaction table')
        
        dblogger.info('mapping customer info with transaction table')

        try:

		#preparing four tables: final_non_indo, customer_non_indo, final_indo, cusotmer_indo
            self.transaction_nonindo['customer_id']=self.transaction_nonindo['customer_id'].apply(lambda x: str(x))
        
            customer_non_indo=self.final_customer_table[self.final_customer_table['indo_type']=='NON_INDO']
         
           
            #############################

		#mapping none indomarte transaction table to the none indomrate customer table by using the methods of left merging 
        
            final_non_indo=self.transaction_nonindo.merge(customer_non_indo[['customer_id','city','province','shop_type', 'customer_address',
                                                    'latitude','longtitude']],how='left',on=['customer_id'])


		
                    
            non_customer_info=[x for x in final_non_indo['customer_id'].unique() if x not in customer_non_indo['customer_id'].unique()]

            dblogger.info('successfully update customer info to transaction table')
        
            df_transaction=final_non_indo.copy()
            df_transaction['Date']=pd.to_datetime(df_transaction['Date'])
    
    
            dblogger.info('creating month, date, year, system date')
            
            df_transaction['Date']=pd.to_datetime(df_transaction['Date'])
            df_transaction['year']=df_transaction['Date'].apply(lambda x: x.year)
            df_transaction['month']=df_transaction['Date'].apply(lambda x: x.month)
            df_transaction['week_of_month']=df_transaction['Date'].apply(lambda x: np.nan if pd.isnull(x) else int(math.ceil((x.day+x.replace(day=1).weekday())/7.0)) )
            df_transaction['system_date']=datetime.date.today()
            df_transaction.loc[df_transaction['city'].isna(),'city']='CITY NA'
            dblogger.info('mapping successfully')
            return df_transaction,non_customer_info#,repeated_customer_list
    
          
        except:
            dblogger.info('please checking the table values')
            
       
        
        
        
    
    
    def mapping_indo_customer(self):


        dblogger.info('creating columns for df_transaction table')
        
        dblogger.info('mapping customer info with transaction table')

        try:
            
            		
            
            		#mapping indomarte transaction table to the indomrate customer table by using the methods of left merging by creating new columns in transaction tables and mapping 
            customer_indo=self.final_customer_table[self.final_customer_table['indo_type']=='INDO']
            
            self.transaction_indo[self.master_list_city],self.transaction_indo[self.master_list_province], self.transaction_indo[self.master_list_shoptype], self.transaction_indo['latitude'], self.transaction_indo['longtitude']=[np.nan,np.nan,np.nan,np.nan,np.nan]
            
            
            non_customer_info=[]
            
            for x in self.transaction_indo['Distribution Center'].unique():
                if  x in customer_indo['customer_name'].unique():
            
                    #final_indo.loc[final_indo['DC']==x,['city','province','store_type', 'customer_address', 'latitude',
                    self.transaction_indo.loc[self.transaction_indo['Distribution Center']==x,[self.master_list_city, self.master_list_province,self.master_list_shoptype, 'latitude',
                                                        'longtitude']]=customer_indo[customer_indo['customer_name']==x][['city','province','shop_type','latitude','longtitude']].iloc[-1].values
                else:
                  
                    non_customer_info.append(x)
                    
            
            
            dblogger.info('successfully update customer info to transaction table')
            
               
            df_transaction=self.transaction_indo.copy()
            df_transaction['Invoice Date']=pd.to_datetime(df_transaction['Invoice Date'])
                            
            
            dblogger.info('creating month, date, year, system date')
            
            df_transaction['Invoice Date']=pd.to_datetime(df_transaction['Invoice Date'])
            df_transaction['year']=df_transaction['Invoice Date'].apply(lambda x: x.year)
            df_transaction['month']=df_transaction['Invoice Date'].apply(lambda x: x.month)
            df_transaction['week_of_month']=df_transaction['Invoice Date'].apply(lambda x: np.nan if pd.isnull(x) else int(math.ceil((x.day+x.replace(day=1).weekday())/7.0)) )
            df_transaction['system_date']=datetime.date.today()
            df_transaction.loc[df_transaction[self.master_list_city].isna(),self.master_list_city]='CITY NA'
            return df_transaction,non_customer_info
            

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
            
            con=create_conn()
            cur = con.cursor()
            rst = table_check(con, command_exists)
            if(rst):
                dblogger.info('table exists')           
                cur = con.cursor()
                
                if 'Unnamed: 0' in self.final_customer_table.columns:
                    self.final_customer_table=self.final_customer_table.drop('Unnamed: 0',1)
                else:
                    pass
                values=self.final_customer_table.values.tolist()
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
                    self.final_customer_table=self.final_customer_table.drop('Unnamed: 0',1)
                else:
                    pass
               # new_customer_table,problematic_table=first_customer_table(self.customer_table,ctable_status)
                values=self.final_customer_table.values.tolist()
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
                """)
            
        con=create_conn()
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
         distribution_center text NOT NULL,
         qty_mc numeric,
         amount numeric,
           
         sku text NOT NULL,
         series_num text NOT NULL,
         qty_packs numeric,
 
         amount_millions numeric,
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
                        FROM   information_schema.tables 
                        WHERE  table_schema = 'public'
                        AND    table_name = 'final_indo'
                        );
                """)
            
        con=create_conn()
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
            


