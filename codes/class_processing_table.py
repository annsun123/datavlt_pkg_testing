# -*- coding: utf-8 -*-
"""
Created on Tue Dec  3 16:52:56 2019

@author: anyan.sun
"""

# -*- coding: utf-8 -*-
"""
Created on Sun Dec  1 15:46:27 2019

@author: anyan.sun
"""

#######Outside Packages ##########
import os 
import re
import json
import requests
import datetime
import pandas as pd 
import numpy as np
from time import sleep
import logging 
import psycopg2
###Self Packages 
from codes.otherFunction import google_geocode1
from codes.class_logging import logging_func
from codes.otherFunction import create_conn, table_check

process=logging_func('table_processing_log',filepath='')
processing_logger=process.myLogger()

class processing:
    def __init__(self,new_customer,ctable_status,customer_num_col,master_list_city,master_list_cname,
                master_list_address,master_list_province,master_list_shoptype,master_list_contact,transaction_file_name,qty_col,
                description_col,date_col,amount_col,booster_conversion,starter_conversion,cred):
        self.new_customer=new_customer
        self.ctable_status=ctable_status
        self.customer_num_col=customer_num_col
        self.master_list_city=master_list_city
        self.master_list_cname=master_list_cname
        self.master_list_address=master_list_address
        self.master_list_province=master_list_province  
        self.master_list_shoptype=master_list_shoptype
        self.master_list_contact=master_list_contact
        self.transaction_file_name=transaction_file_name
        self.qty_col=qty_col
        self.amount_col=amount_col
        self.description_col=description_col
        self.date_col=date_col  
        self.booster_conversion=booster_conversion #720, 
        self.starter_conversion=starter_conversion #120)
        self.cred=cred



    def first_customer_table(self):
        #new_customer,ctable_status):
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
            processing_logger.info('table exists')   

            processing_logger.info('loading old latest customer table')

            command='select * from customer_table where updated_daet = (select max(updated_daet) from customer_table)'


            conn=create_conn()
            cur=conn.cursor()
            cur.execute(command)
            table=cur.fetchall()
            cur.close()
            curs=conn.cursor()

            curs.execute("Select * FROM customer_table LIMIT 0")
            colnames = [desc[0] for desc in curs.description]
            cur.close()
            conn.close()

            df_customer_old=pd.DataFrame(table, columns=colnames)
            processing_logger.info('old latest customer table loaded successfully')


            if self.ctable_status=='not_uploaded':
                final_customer_table=df_customer_old.copy()
            else:  
                
                try:

                    processing_logger.info('processing new customer table')
                    self.new_customer=self.new_customer.rename(columns={self.customer_num_col:'customer_id'})
                      
                    processing_logger.info('new customer has columns: '+ ', '.join(self.new_customer.columns))
                    processing_logger.info('left merging new customer_table to old_customer table')
                    df_customer_old['customer_id']=df_customer_old['customer_id'].astype(int)
                    df_customer=self.new_customer.merge(df_customer_old,how='left',on='customer_id')


                    for i in [self.master_list_city,self.master_list_cname,self.master_list_address,self.master_list_contact]:

                        df_customer[i]=df_customer[i].str.upper()


                    processing_logger.info('merging successfully')

                    processing_logger.info('filter out customer with none city or none address')
                    df_customer_api=df_customer[~(df_customer[self.master_list_address].isna() | df_customer[self.master_list_city].isna())]
                    df_customer_api['full_address']=df_customer_api[[self.master_list_address,self.master_list_city]].apply(lambda x: ', '.join(x)+', Indonesia', axis=1)
                    processing_logger.info('acquiring customer address')
                    table,error_address=google_geocode1(df_customer_api[df_customer_api['longtitude'].isnull()],'full_address','customer_id',self.cred)
                    #table,error_address=google_geocode1(df_customer_api[df_customer_api['longtitude'].isna()],'full_address','customer_id',cred='AIzaSyAoOVNRNLd8B3470_4wR1lPezejA3_zFFI')
                    processing_logger.info('updating customers address')
                    df_customer_api.loc[df_customer_api['longtitude'].isnull(),['formal_address','longtitude','latitude']]=table[['Formatted Address','Longitude','Latitude']].values
                   # df_customer_api.loc[df_customer_api['longtitude'].isna(),['formal_address','longtitude','latitude']]=table[['Formatted Address','Longitude','Latitude']].values
                    processing_logger.info('preparing problematic table')
                    problematic_table=df_customer[pd.isnull(df_customer[self.master_list_address]) | pd.isnull(df_customer[self.master_list_city])].append(df_customer.iloc[error_address])
                   # problematic_table=df_customer[df_customer[self.master_list_address].isna() | df_customer[self.master_list_city].isna()].append(df_customer.iloc[error_address])
                    problematic_table['error_type']='missing city or address'           
                    problematic_table.loc[error_address,'error_type']='wrong format address' 
                    processing_logger.info('prepare the final table')
                    processing_logger.info('merging the problematic table with api')
                    final_customer_table=df_customer_api.merge(problematic_table,how='outer').merge(table,how='outer')
                    processing_logger.info('final table error_status update')
                   # final_customer_table.loc[final_customer_table['error_type'].isna(),'error_type']='Y'
                    final_customer_table.loc[final_customer_table['error_type'].isnull(),'error_type']='Y'
                    final_customer_table.loc[final_customer_table[self.master_list_city].isnull(),self.master_list_city]='CITY NA'
                   # final_customer_table.loc[final_customer_table[self.master_list_city].isna(),self.master_list_city]='CITY NA'
                    final_customer_table,problematic_table=[x.drop('Unnamed: 0',1)  if 'Unnamed: 0' in x  else x for x in [final_customer_table,problematic_table]] 

                    processing_logger.info('selecting columns')        

                    final_customer_table=final_customer_table[['customer_id',self.master_list_cname, self.master_list_address, 'formal_address',   
                                                                                    self.master_list_city,self.master_list_province, self.master_list_shoptype,self.master_list_contact,'longtitude','latitude','error_type']]

                    final_customer_table['updated_date']=datetime.date.today()
                    processing_logger.info('successfully process customer table')            

                except IndexError as e:
                    processing_logger.error(e)


        else:
            processing_logger.info('table does not exists') 
            if self.ctable_status=='not_uploaded':
                processing_logger.info('the table is not uploaded')
            else:         
                try:
                    #f_customer['full_address']=df_customer[[master_list_address,master_list_city]].apply(lambda x: 'Jarkata, Indonesia' if x.isnull().all() else ', '.join(x.dropna())+', Indonesia', axis=1)
                    processing_logger.info('capitablizing tables ') 
                    self.new_customer=self.new_customer.rename(columns={self.customer_num_col:'customer_id'})
                    for i in [self.master_list_city,self.master_list_cname,self.master_list_address,self.master_list_contact]:

                        self.new_customer[i]=self.new_customer[i].str.upper()

                    processing_logger.info('selecting out non address/city columns') 
                    df_customer_api=self.new_customer[~(pd.isnull(self.new_customer[self.master_list_address])| pd.isnull(self.new_customer[self.master_list_city]))]  
                    #df_customer_api=self.new_customer[~(self.new_customer[self.master_list_address].isna() | self.new_customer[self.master_list_city].isna())]
                    df_customer_api['full_address']=df_customer_api[[self.master_list_address,self.master_list_city]].apply(lambda x: ', '.join(x)+', Indonesia', axis=1)
                    processing_logger.info('searching api ') 
                    table,error_address=google_geocode1(df_customer_api,'full_address','customer_id',self.cred)
                    processing_logger.info('preparing error table ') 
                    df_cusomter=self.new_customer.copy()
                    problematic_table=df_cusomter[pd.isnull(df_cusomter[self.master_list_address]) | pd.isnull(df_cusomter[self.master_list_city])].append(df_cusomter.iloc[error_address])
                    #problematic_table=df_cusomter[df_cusomter[self.master_list_address].isna() | df_cusomter[self.master_list_city].isna()].append(df_cusomter.iloc[error_address])
                    problematic_table['error_type']='missing city or address'
                    problematic_table.loc[error_address,'error_type']='wrong format address' 

                    new_table=df_customer_api.merge(table)
                    new_table.merge(problematic_table,on='customer_id')
                    final_customer_table=df_customer_api.merge(problematic_table,how='outer').merge(table,how='outer')
                    final_customer_table.loc[final_customer_table['error_type'].isnull(),'error_type']='Y'
                    final_customer_table,problematic_table=[x.drop('Unnamed: 0',1)  if 'Unnamed: 0' in x  else x for x in [final_customer_table,problematic_table]]
    
                    final_customer_table.loc[final_customer_table[self.master_list_city].isnull(),self.master_list_city]='CITY NA'
                    processing_logger.info('renaming tables') 
                    final_customer_table=final_customer_table[['customer_id',self.master_list_cname, self.master_list_address, 'Formatted Address',   
                                                                                   self.master_list_city,self.master_list_province, self.master_list_shoptype,self.master_list_contact,'Longitude','Latitude','error_type']]
                    final_customer_table.columns=['customer_id',self.master_list_cname, self.master_list_address, 'formal_address',   
                                                                                    self.master_list_city, self.master_list_province, self.master_list_shoptype,self.master_list_contact,'longtitude','latitude','error_type']
                    final_customer_table['updated_date']=datetime.date.today()
                    processing_logger.info('processing customer table successfully') 
                except IndexError as e:
                    processing_logger.error(e)


        return final_customer_table,problematic_table
                      
    
    def processing_history_excepterrorV2(self):
        
           # transaction_file_name,qty_col, description_col, date_col,booster_conversion=720, starter_conversion=120):
        try:           
            df=pd.read_excel(self.transaction_file_name)

            row_skip=df[df.isin([self.date_col]).any(axis=1)].index[0]+1
            df_daily=pd.ExcelFile(self.transaction_file_name)
            df_daily=df_daily.parse('Sheet1',skiprows=row_skip)
            day_fill=df_daily.drop(df_daily[df_daily[self.customer_num_col]=='Total'].index)
    
            day_fill[self.date_col]=day_fill[self.date_col].fillna(method='ffill')
            
            day_fill_test1=day_fill.dropna(axis=1,how='all',inplace=False)
            
            index_list=np.where(~day_fill_test1.iloc[0].isna())[0]
            
            
            sku_indval=[(ind,vale) for ind, vale in enumerate(day_fill_test1.columns[index_list])  if 'Unnamed' not in vale]
            
            for i in range(len(sku_indval)):
                day_fill_test1.insert(int(index_list[sku_indval[i][0]])+i,'sku_name'+str(i),sku_indval[i][1])
            index_list2=np.where(~day_fill_test1.iloc[0].isna())[0]
            day_fill_test1=day_fill_test1.drop(day_fill_test1.index[0])
            day_fill_test1=day_fill_test1.drop(day_fill_test1.index[-1])
            
            
            #df_final_test.append(day_fill_test1[day_fill_test1.columns[index_list2[i:i+3]]])
            df_final_test=pd.DataFrame()
            for i in range(0,len(index_list2),3):
                
                df_final_list_test=[]
                df_final_list_test.append(day_fill_test1.iloc[:,:3])
                df_final_list_test.append(day_fill_test1[day_fill_test1.columns[index_list2[i:i+3]]])
                data=pd.concat(df_final_list_test,axis=1)
                data.columns=['Date','customer_id','customer_name','sku_type','qty(MC)','amount']
                df_final_test=df_final_test.append(data)
            
            
            df_final_test['qty(MC)']=df_final_test['qty(MC)'].apply(lambda x : np.nan if x =='-' else x)
            df_final_test['amount']=df_final_test['amount'].apply(lambda x : np.nan if x =='-' else x)
            df_final_test=df_final_test.dropna(subset=['qty(MC)','amount'])
            
            
            ########### sku name Processing #################
            

            df_final_test['sku']=df_final_test['sku_type'].apply(lambda x: 'BOOSTER A' if ' A' in x.upper() else('BOOSTER B'if ' B'  in x.upper() else ('STARTER DECK' if 'STARTER' in x.upper() else x)))
            df_final_test['series']= df_final_test['sku_type'].apply(lambda x: re.search(r'\d',x).group(0) if bool(re.search(r'\d',x)) else '1')
            processing_logger.info('sereis contains values: ' + ', '.join(df_final_test['series'].unique()))
            df_final_test['qty(pakcs)']=df_final_test.apply(lambda x: x['qty(MC)']*self.booster_conversion if x['sku']!='STARTER DECK' else x['qty(MC)']*self.starter_conversion,axis=1)
            processing_logger.info('table contains columns: ' + ', '.join(df_final_test.columns))
            processing_logger.info('table pre-processed successfully')
            return df_final_test 
        
        except (IndexError, KeyError) as e:
            processing_logger.error('Please checking description name and sku names or Table format has been changed')
        
        #,problematic_values
