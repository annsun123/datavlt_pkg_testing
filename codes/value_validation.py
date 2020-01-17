# -*- coding: utf-8 -*-
"""
Created on Fri Nov 29 11:20:10 2019

@author: anyan.sun
"""

import json 
import pandas as pd 
from codes.class_logging import logging_func


validation=logging_func('value_validation_log',filepath='')
validationlogger=validation.myLogger()

def problem_values(df_final):
    
    df_final['Date']=df_final['Date'].astype(str)
    problematic_values=df_final[(df_final['qty(MC)']<0) | (df_final[['sku','series','customer_name','Date','system_date']].isnull().any(axis=1))][['customer_name','sku','Date','qty(MC)']]
    
    problematic_values=json.loads(problematic_values.astype(object).where(problematic_values.notnull(),None).to_json(orient='records'))
    if len(problematic_values)>0:
        validationlogger.error('existing nan or negative quantity values')
        
        #sending the problematic_value through the email
    else:

        validationlogger.info('all values are valid')
    validationlogger.error(problematic_values)
    validationlogger.error('the index {} contains null'.format(list(df_final[df_final[['sku','series','customer_name','Date','system_date']].isnull().any(axis=1)].index)))
    return problematic_values