# -*- coding: utf-8 -*-
"""
Created on Thu Feb 27 17:11:03 2020
  #else:
            #conn = create_conn()
            #cur = conn.cursor()
            #old_indo = pd.read_sql_query("select * from final_indo \
                                     #  where system_date=(select max(system_date) \
                                      # from final_indo)",\
                                                  #     conn)          
            #cur.close()
            #conn.close()                            
            #df_final_indo.columns = old_indo.columns      
            #df_final_indo['invoice_date'] = df_final_indo['invoice_date'].apply(lambda x: x.date())
                            
            #idxs_indo =  list(zip(old_indo.invoice_date, old_indo.sku, \
                            #old_indo.amount))  
            #df_final_indo = df_final_indo[~pd.Series(list(zip(df_final_indo.invoice_date, df_final_indo.sku, \
                              #df_final_indo.amount)), index=df_final_indo.index).isin(idxs_indo)]                              
    
@author: anyan.sun
"""
import datetime
import pandas as pd 
from codes.otherFunction import creating_plot, item_creating, create_conn, table_existance
def rm_duplic_df(df_type, table):
    
    conn = create_conn()
    status = table_existance(conn, df_type)
    if status:

        conn = create_conn()
        cur = conn.cursor()
        
        old_table = pd.read_sql_query("select * from " + df_type+ \
                                            " where system_date=(select max(system_date) from " +\
                                            df_type + " )",\
                                                           conn)                                          
        cur.close()
        conn.close()                     
      
        table.columns = old_table.columns         
        if df_type=='final_nonindo':
            customer='customer_name'
        else:
            customer='distribution_center'
        table['invoice_date'] = table['invoice_date'].apply(lambda x: x.date())
        idxs_table =  list(zip(old_table.invoice_date, old_table.sku, \
                        old_table.amount,old_table[customer]))            
        table = table[~pd.Series(list(zip(table.invoice_date, table.sku, \
                          table.amount)), index=table.index).isin(idxs_table)]
       
                                 
       
      
    else:
          table=table
        
    return table