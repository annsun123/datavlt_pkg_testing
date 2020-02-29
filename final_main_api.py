# -*- coding: utf-8 -*-
"""
Created on Fri Feb  7 14:53:25 2020

@author: anyan
"""
import os
import pandas as pd
import json
import datetime
import shutil
import numpy as np
import logging
from psycopg2.extras import Json
import warnings
warnings.filterwarnings('ignore')
#self build library
from json_function.codes.supporting_function import value_existance
from json_function.codes.json_generating import creating_json_nonindo
from json_function.codes.json_generating_indo import creating_json_indo
from json_function.codes.json_generating_rfm import creating_json_rfm
from codes.gdrive.googleFiling import downloadfiles 
from codes.otherFunction import creating_plot, item_creating, create_conn, table_existance
from codes.class_logging import logging_func
from codes.value_validation import problem_values
from codes.class_dbinsert import inserting_table,insert_nonindo_table,insert_indo_table, insert_json_table
from codes.class_processing_table import processing, processing_history_indo
from codes.class_sending_email import sending_emails
from codes.rm_dupli_df import rm_duplic_df

def main():

    logger=logging_func('main_log',filepath='')
    mainlogging=logger.myLogger()

    with open('./json_credential/overall_credential.json') as file:
        credentials = json.load(file)
    file.close()


    customer_num_col = 'Customer No.'
    master_list_city = 'City'
    master_list_cname = 'Customer Name'
    master_list_address = 'Address 1'
    master_list_contact = 'Contact Name'
    qty_col = 'Qty Out (MC)'
    description_col = 'Description'
    date_col = 'Date'
    amount_col = 'amount'
    master_list_province = 'Province'
    master_list_shoptype = 'Store Type'
    booster_conversion = 720
    starter_conversion = 120
    dc_col = 'BranchName'
    
    cred = credentials['google_api']['geo_api']
    from_address = credentials['mailing_credential']['from_address']
    receiving_address = credentials['mailing_credential']['receiving_address']
    user_name = credentials['mailing_credential']['user_name']
    password = credentials['mailing_credential']['password']
    delete_status = credentials['file_delete_from_googledrive']['not_delete']
    
    try:
        downloadPath = 'files/'
        file_downloaded, cnt = downloadfiles(downloadPath,
                                             credentials['google_api'],
                                             delete_status)
        customer_indomaret = customer_nonindomaret = transaction_indo = transaction_nonindo = ''
        for i in file_downloaded:
            file_name = i.split('.')[0]
            item_status = {'table_name': file_name,
                           'process_date': datetime.date.today(),
                           'process_status': 'processed'}
            creating_plot('status_table',
                          item_status,
                          table_creating_command='(process_date date NOT NULL,process_status text NOT NULL,Table_name text NOT NULL)')
            if 'customer' in file_name.lower():
                customer_nonindomaret = pd.read_excel(downloadPath + i, sheet_name='Non-Indomaret')
                customer_indomaret = pd.read_excel(downloadPath + i, sheet_name='Indomaret')
                customer_indomaret = pd.ExcelFile(downloadPath + i)
                
                row_skip = 0
                customer_indomaret = customer_indomaret.parse('Indomaret', skiprows=row_skip)
                
                if 'Customer Number' in customer_indomaret.iloc[:,0].values:
                    row_skip = customer_indomaret.iloc[:,0].values.tolist().index('Customer Number')
                    customer_indomaret.columns=customer_indomaret.iloc[row_skip,:]
                    customer_indomaret = customer_indomaret.drop(customer_indomaret.index[:row_skip+1])
            elif 'indomaret' in file_name.lower():
                transaction_indo = i
            else:
                transaction_nonindo = i
        if any(['customer' in x.lower().split('.')[0] for x in file_downloaded]):
            ctable_status = 'uploaded'
        else:
            ctable_status = 'not_uploaded'
        mainlogging.info('prepare cusotmer_table')

        processing_cl = processing(customer_indomaret = customer_indomaret,
                                   customer_nonindomaret = customer_nonindomaret,
                                   ctable_status = ctable_status,
                                   customer_num_col = customer_num_col,
                                   master_list_city=master_list_city,
                                   master_list_cname=master_list_cname,
                                   master_list_address=master_list_address,
                                   master_list_province=master_list_province,
                                   master_list_shoptype=master_list_shoptype,
                                   master_list_contact=master_list_contact,
                                   transaction_nonindo_file=downloadPath + transaction_nonindo,
                                   qty_col=qty_col,
                                   description_col=description_col,
                                   date_col=date_col,
                                   amount_col=amount_col,
                                   booster_conversion=booster_conversion,
                                   starter_conversion=starter_conversion,
                                   cred=cred)

        mainlogging.info('processing customer table')
        customer_table,customer_error = processing_cl.first_customer_table()
        customer_table.columns = ['customer_id',
                                  'customer_name',
                                  'customer_address',
                                  'formal_address',
                                  'city',
                                  'province',
                                  'shop_type',
                                  'contact_name',
                                  'longtitude',
                                  'latitude',
                                  'error_type',
                                  'indo_type',
                                  'updated_date']

        if len(customer_error)!=0:
            customer_error.to_excel('log/customer_address_error.xlsx')
        mainlogging.info('processing transaction table')
        
        mainlogging.info('processing transaction table')
        if transaction_nonindo !='':
            df_nonindo = processing_cl.processing_history_nonindo()
            df_nonindo['amount_million'] = df_nonindo['amount'] / 1000000
            df_nonindo = df_nonindo[['sku',
                                                   'series',
                                                   'customer_id',
                                                   'customer_name',
                                                   'Date',
                                                   'qty(MC)',
                                                   'qty(pakcs)',
                                                   'amount',
                                                   'amount_million']]
            
            nonindo_table_upload = 'nonindo_table upaded'
        else:
            mainlogging.error('non-indomarate data not uploaded ')
            nonindo_table_upload='nonindo_table not upaded'
            df_nonindo=[]
            
        if transaction_indo !='':
            transaction_indo_file = downloadPath + transaction_indo
            df_indo = processing_history_indo(transaction_indo_file,
                                                   booster_conversion,
                                                   starter_conversion)

            df_indo['amount_million'] = df_indo['amount'] / 1000000
            indo_table_upload = 'indo_table upaded'
        else:
             mainlogging.error('the indomarate data is not updated')
             indo_table_upload='indo_table not upaded'
             df_indo=[]
            
             
        if len(customer_error) > 0:
            mainlogging.error('customer table contains error/none values')
        else:
            mainlogging.info('customer table does not contain error/non values')

                                        
        mainlogging.info('loading inserting functions')
        db = inserting_table(df_nonindo,
                             df_indo,
                             customer_table,
                             master_list_city = master_list_city,
                             master_list_province = master_list_province,
                             master_list_shoptype = master_list_shoptype,
                             master_list_address = master_list_address,
                             master_list_contact = master_list_contact,
                             dc_col = dc_col)
        mainlogging.info('inserting cusotmer table')
        db.inserting_customer_table()
        mainlogging.info('mapping customer table with transaction table, preparaing final table')
        
        if len(transaction_nonindo)!=0:
            df_final_nonindo, missing_customer_info1 = db.mapping_nonindo_customer()
            df_final_nonindo = df_final_nonindo.rename(columns={'Date':'invoice_date', 'series':'series_num', \
                                                              'qty(MC)':'qty_mc'})
            mainlogging.info('removing duplicates in non-indo table')
          
            df_final_nonindo =  rm_duplic_df('final_nonindo', df_final_nonindo) 
            problem_nonindo = problem_values(df_final_nonindo)
            df_final_nonindo['invoice_date'] = pd.to_datetime(df_final_nonindo['invoice_date'])
            from_six_nonindo = (df_final_nonindo['invoice_date'].max()\
                              -datetime.timedelta(6*365/12)).date().strftime('%Y-%m-%d')    
            from_three_nonindo = (df_final_nonindo['invoice_date'].max()\
                              -datetime.timedelta(3*365/12)).date().strftime('%Y-%m-%d')    
            from_one_nonindo = (df_final_nonindo['invoice_date'].max()\
                         -datetime.timedelta(1*365/12)).date().strftime('%Y-%m-%d')  
            if len(problem_nonindo) > 0:
                mainlogging.error('non_indo transaction contains error/none values')
            else:
                mainlogging.info('non_indo transactionn does not contain error/non values')
                
            mainlogging.info('inserting nonindo final table')
            insert_nonindo_table(df_final_nonindo)
            
         
            to_date=''
            from_date=''
            
            for date_opt in zip((from_six_nonindo, from_three_nonindo, from_one_nonindo),('6_month', '3_month', '1_month')):
                conn=create_conn()
                cur = conn.cursor()
                #from_date_nonindo = date_opt[0]
                mainlogging.info('read old data from database')
                final_table_nonindo = pd.read_sql_query("SELECT * from final_nonindo where invoice_date >= '" +  date_opt[0] + "'", conn)
                cur.close()
                conn.close()
                # there are 11 pieces json for graphs in the json_list    
                mainlogging.info('generating non_indo json files')
                json_nonindolist = creating_json_nonindo(final_table_nonindo)
    
                db_json_nonindotable=[]
                for i in json_nonindolist:
                       
                    db_json_nonindotable.append([i['graph_description'], 'non_indo', \
                                        date_opt[1], to_date, from_date, \
                                         datetime.date.today(), Json(i)])
                insert_json_table(db_json_nonindotable)
          
        else:
            missing_customer_info1=['']
            df_final_nonindo=[]
            problem_nonindo=[]
            
        if len(transaction_indo)!=0:
            df_final_indo, missing_customer_info2 = db.mapping_indo_customer()
            df_final_indo = df_final_indo.rename(columns={'Invoice Date':'invoice_date', 'Series':'series_num', \
                                                              'qty(MC)':'qty_mc', 'Distribution Center':'distribution_center',\
                                                             'SKU':'sku' })
            mainlogging.info('removing duplicates in indo table')    
            df_final_indo =  rm_duplic_df('final_indo', df_final_indo) 
            problem_indo = problem_values(df_final_indo)
            df_final_indo['invoice_date'] = pd.to_datetime(df_final_indo['invoice_date'])
            from_six_indo = (df_final_indo['invoice_date'].max()\
                              -datetime.timedelta(6*365/12)).date().strftime('%Y-%m-%d')    
            from_three_indo = (df_final_indo['invoice_date'].max()\
                              -datetime.timedelta(3*365/12)).date().strftime('%Y-%m-%d')    
            from_one_indo = (df_final_indo['invoice_date'].max()\
                         -datetime.timedelta(1*365/12)).date().strftime('%Y-%m-%d') 
            
            if len(problem_indo) > 0:
                mainlogging.error('indo transaction contains error/none values')
            else:
                mainlogging.info('indo transactionn does not contain error/non values')
                 
            mainlogging.info('inserting indo final table')
            insert_indo_table(df_final_indo)    
            
            mainlogging.info('preparaing final json')
             
            mainlogging.info('read old data from database')
            
            to_date=''
            from_date=''
            for date_opt in zip((from_six_indo, from_three_indo, from_one_indo),('6_month','3_month','1_month')):
                
                conn=create_conn()
                cur = conn.cursor()
                final_table_indo = pd.read_sql_query("SELECT * from final_indo where invoice_date >= '" +  date_opt[0] + "'", conn)
                cur.close()
                conn.close()
               
                # there are 10 pieces json for graphs in the json_list    
                mainlogging.info('generating non_indo json files')
                json_indolist = creating_json_indo(final_table_indo)

                db_json_indotable=[]
                for i in json_indolist:
                       
                    db_json_indotable.append([i['graph_description'], 'indo', date_opt[1],\
                                         to_date, from_date, datetime.date.today(), Json(i)])
                 
                insert_json_table(db_json_indotable)
            processing_status='default json table insert correctly'
      

        else:
            missing_customer_info2=['']
            df_final_indo=[]
            problem_indo=[]
            
            
        mainlogging.info('successfully insert all tables')    
        missing_customer_info = missing_customer_info1 + missing_customer_info2
       
        if len(missing_customer_info) > 0:
            mainlogging.error('sales data includes customer not in customer master list')
        else:
            mainlogging.info('customer master list covers all customer in sales list ')
   

     #   from_date_input = (datetime.date.today() -datetime.timedelta(6*365/12)).strftime('%Y-%m-%d')  

    # error rfm creating 
        if len(problem_indo) >= 0:
            item_mapping = item_creating(problem_indo,'transaction_error')    
            creating_plot('error_report',item_mapping,table_creating_command='( Processing_Date date NOT NULL, Type text NOT NULL, output_json JSONB NOT NULL)')
        if len(problem_nonindo) >= 0:
            item_mapping = item_creating(problem_nonindo,'transaction_error')    
            creating_plot('error_report',item_mapping,table_creating_command='( Processing_Date date NOT NULL, Type text NOT NULL, output_json JSONB NOT NULL)')
        
        if len(missing_customer_info)>=0:    
            missing_json_dump=json.dumps(missing_customer_info)
            customer_missing=item_creating(missing_json_dump,'missing_customer_error')
            creating_plot('error_report',customer_missing,table_creating_command='( Processing_Date date NOT NULL, Type text NOT NULL, output_json JSONB NOT NULL)')
        try:   
            mainlogging.info('preparaing customer error json')
            customer_error=customer_error.astype(object).where(customer_error.notnull(), None)
        
            customer_error_json=customer_error[['customer_id',master_list_cname, master_list_city,master_list_address,'error_type']].to_dict(orient='records')
            customer_rfm=item_creating(customer_error_json, 'cusotmer_error_json')
            creating_plot('error_report',customer_rfm,table_creating_command= '(Processing_Date date NOT NULL, Type text NOT NULL,  output_json JSONB)')
        except:
            mainlogging.info('non customer has errorous address')    
            
        
        mainlogging.info('preparaing json files')    
        mainlogging.info('preparaing customer profiling clustering')
        
        
        # creating RFM JSON
        rfm_json_table=creating_json_rfm()
        insert_json_table(rfm_json_table)          

             
     # non-indo and indo json creation
       
        for filename in file_downloaded:
            os.unlink(downloadPath+filename)
        return indo_table_upload+nonindo_table_upload+processing_status

    except Exception as e:
        
        mainlogging.error(e)
        
    finally:
        mainlogging.info('sending emails')
       
      #  sending_emails(from_address=from_address,user_name=user_name,
                    #   password=password,receiving_address=receiving_address)
 
        #for i in os.listdir('log'):
         #   shutil.copy('log/'+i, 'historical files')
            #os.unlink('log/'+i)
 
        mainlogging.info("Sending logs to anyan.sun@datavlt.com through emial")        
  
if __name__ == '__main__':
    main()
