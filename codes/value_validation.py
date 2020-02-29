import json
from codes.class_logging import logging_func
validation = logging_func('value_validation_log', filepath='')
validationlogger = validation.myLogger()


def problem_values(df_final, df_type):
    if df_type=='indo':
        
        cname_col = 'distribution_center'
        sku_col = 'sku'
        series_col = 'series_num'
    else:
        
        cname_col = 'customer_name'
        sku_col = 'sku'
        series_col = 'series_num'

    df_final['invoice_date'] = df_final['invoice_date'].astype(str)
    problematic_values = df_final[
        (df_final['qty_mc'] < 0) | (df_final[[sku_col,
                                               series_col,
                                               cname_col,
                                               'invoice_date',
                                               'system_date']].isnull().any(axis=1))
    ][[cname_col,
       sku_col,
       'invoice_date',
       'qty_mc']]

    problematic_values = json.loads(
        problematic_values.astype(object).where(problematic_values.notnull(), None).to_json(orient='records')
    )
    if len(problematic_values) > 0:
        validationlogger.error('existing nan or negative quantity values')

# sending the problematic_value through the email
    else:
        validationlogger.info('all values are valid')
        validationlogger.error(problematic_values)
        validationlogger.error(
            'the index {} contains null'.format(list(df_final[df_final[[sku_col,
                                                                        series_col,
                                                                        cname_col,
                                                                        'invoice_date',
                                                                        'system_date']].isnull().any(axis=1)].index))
        )
    return problematic_values
