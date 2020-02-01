import json 
from codes.class_logging import logging_func
validation = logging_func('value_validation_log',filepath='')
validationlogger = validation.myLogger()

def problem_values(df_final):
    if 'Distribution Center' in df_final.columns:
        date_col = 'Invoice Date'
        cname_col = 'Distribution Center'
        sku_col = 'SKU'
        series_col = 'Series'
    else:
        date_col = 'Date'
        cname_col = 'customer_name'
        sku_col = 'sku'
        series_col = 'series'

    df_final[date_col] = df_final[date_col].astype(str)
    problematic_values = df_final[
        (df_final['qty(MC)'] < 0) | (df_final[[sku_col, series_col,
                                               cname_col, 
                                               date_col,
                                               'system_date']].isnull().any(axis=1))
    ][[cname_col, sku_col, date_col, 'qty(MC)']]
    
    problematic_values = json.loads(
        problematic_values.astype(object).where(problematic_values.notnull(),None).to_json(orient='records')
    )
    if len(problematic_values) > 0:
        validationlogger.error('existing nan or negative quantity values')
      
#sending the problematic_value through the email
    else:
        validationlogger.info('all values are valid')
        validationlogger.error(problematic_values)
        validationlogger.error(
            'the index {} contains null'.format(list(
                df_final[df_final[[sku_col,
                                   series_col,
                                   cname_col, date_col,
                                   'system_date']].isnull().any(axis=1)].index)
                                               )
        )
    return problematic_values
