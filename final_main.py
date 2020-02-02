import os
import pandas as pd
import json
import datetime
from codes.gdrive.googleFiling import downloadfiles 
from codes.otherFunction import creating_plot, item_creating, create_conn
from codes.class_logging import logging_func
from codes.value_validation import problem_values
from codes.class_dbinsert import inserting_table, insert_nonindo_table, insert_indo_table
from codes.class_processing_table import processing, processing_history_indo
from codes.class_rfm import createRFM_dataset, clustering, defineCluster
from codes.class_sending_email import sending_emails
import logging
logger = logging_func('main_log', filepath='')
mainlogging = logger.myLogger()


def main():

    with open('./json_credential/overall_credential.json') as file:
        credentials = json.load(file)
    file.close()
    logging.getLogger().setLevel(logging.INFO)
    # Creating cron job

    logging.info('Starting APScheduler...')
    from apscheduler.schedulers.blocking import BlockingScheduler
    sched = BlockingScheduler()
    sched.daemonic = False
    @sched.scheduled_job(trigger='cron',
                         hour=credentials['scheduler_time']['hour'],
                         minute=credentials['scheduler_time']['minute'])
    
    def mainsub():
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
                    if len(customer_indomaret[customer_indomaret.isin(['Customer Number']).any(axis=1)]) > 0:
                        row_skip = customer_indomaret[customer_indomaret.isin(['Customer Number']).any(axis=1)].index[0] + 1
                    else:
                        row_skip = 0
                    customer_indomaret = pd.ExcelFile(downloadPath + i)
                    customer_indomaret = customer_indomaret.parse('Indomaret', skiprows=row_skip)
                elif 'indomaret' in file_name.lower():
                    transaction_indo = i
                else:
                    transaction_nonindo = i
            if any(['customer' in x.lower().split('.')[0] for x in file_downloaded]):
                ctable_status = 'uploaded'
            else:
                ctable_status = 'not_uploaded'
            mainlogging.info('prepare cusotmer_table')

            processing_cl = processing(customer_indomaret=customer_indomaret,
                                       customer_nonindomaret=customer_nonindomaret,
                                       ctable_status=ctable_status,
                                       customer_num_col=customer_num_col,
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

            customer_error.to_excel('log/customer_address_error.xlsx')
            mainlogging.info('processing transaction table')
            transaction_nonindo = processing_cl.processing_history_nonindo()
            transaction_indo_file = downloadPath + transaction_indo
            transaction_indo = processing_history_indo(transaction_indo_file,
                                                       booster_conversion,
                                                       starter_conversion)

            if len(customer_error) > 0:
                mainlogging.error('customer table contains error/none values')
            else:
                mainlogging.info('customer table does not contain error/non values')

            transaction_nonindo['amount_million'] = transaction_nonindo['amount'] / 1000000
            transaction_indo['amount_million'] = transaction_indo['Amount'] / 1000000
            transaction_nonindo = transaction_nonindo[['sku',
                                                       'series',
                                                       'customer_id',
                                                       'customer_name',
                                                       'Date',
                                                       'qty(MC)',
                                                       'qty(pakcs)',
                                                       'amount',
                                                       'amount_million']]

            mainlogging.info('loading inserting functions')
            db = inserting_table(transaction_nonindo,
                                 transaction_indo,
                                 customer_table,
                                 master_list_city=master_list_city,
                                 master_list_province=master_list_province,
                                 master_list_shoptype=master_list_shoptype,
                                 master_list_address=master_list_address,
                                 master_list_contact=master_list_contact)

            mainlogging.info('mapping customer table with transaction table, preparaing final table')
            df_final_nonindo, missing_customer_info1 = db.mapping_nonindo_customer()
            df_final_indo, missing_customer_info2 = db.mapping_indo_customer()
            missing_customer_info = missing_customer_info1 + missing_customer_info2
            if len(missing_customer_info) > 0:
                mainlogging.error('sales data includes customer not in customer master list')
            else:
                mainlogging.info('customer master list covers all customer in sales list ')
            mainlogging.info('inserting cusotmer table')
            db.inserting_customer_table()
            mainlogging.info('inserting final table')
            insert_nonindo_table(df_final_nonindo)
            insert_indo_table(df_final_indo)
            mainlogging.info('error catching with final table')

            transaction_problem_table = problem_values(df_final_nonindo) + problem_values(df_final_indo)
            if len(transaction_problem_table) > 0:
                mainlogging.error('transactions contains error/none values')
            else:
                mainlogging.info('transaction does not contain error/non values')

#ERROR ITEM CREATION

            item_mapping = item_creating(transaction_problem_table,
                                         'transaction_error')    
            creating_plot('error_report',
                          item_mapping,
                          table_creating_command='( Processing_Date date NOT NULL, Type text NOT NULL, output_json JSONB NOT NULL)')

            missing_json_dump = json.dumps(missing_customer_info)
            customer_missing = item_creating(missing_json_dump,
                                             'missing_customer_error')
            creating_plot('error_report',
                          customer_missing,
                          table_creating_command='( Processing_Date date NOT NULL, Type text NOT NULL, output_json JSONB NOT NULL)')

            mainlogging.info('preparaing json files')
            mainlogging.info('preparaing customer profiling clustering')

            conn=create_conn()
            cur = conn.cursor()
            final_table = pd.read_sql_query("SELECT * from final_nonindo", conn)
            cur.close()
            conn.close()

            raw_ds = final_table.copy()

            if 'Unnamed: 0' in raw_ds:
                raw_ds.drop('Unnamed: 0', axis=1, inplace=True)

            raw_ds['amount'] = raw_ds['amount'] / 1000000
            raw_ds['invoice_date'] = pd.to_datetime(raw_ds['invoice_date'])
            raw_ds['Avg_order_gap'] = (raw_ds['invoice_date'].max() - raw_ds['invoice_date']).dt.days

            str_cols = ['Frequency', 'Monetary', 'Avg_order_gap', 'Recency']
            df_rfm_overall = createRFM_dataset(raw_ds)
            df_rfm_overall['cluster'] = clustering(df_rfm_overall, str_cols, "algo")
            str_cluter_def = ['Star', 'Cash Cows', 'Question Marks', 'Dogs']
            df_rfm_overall = defineCluster(df_rfm_overall, str_cluter_def)
            final_table[['system_date', 'invoice_date']] = final_table[['system_date',
                                                                        'invoice_date']].astype(str)
            df_rfm_output = final_table.merge(df_rfm_overall[['customer_id', 'cluster']],
                                              how='left',
                                              on='customer_id')
            mainlogging.info('preparaing final json')

            final_table = final_table.astype(object).where(final_table.notnull(), None)

            df_final_indo[['system_date', 'Invoice Date']] = df_final_indo[['system_date',
                                                                           'Invoice Date']].astype(str)
            df_final_indo[['longtitude', 'latitude']] = df_final_indo[['longtitude',
                                                                      'latitude']].astype(float)
            df_final_indo = df_final_indo.astype(object).where(df_final_indo.notnull(), None)
            indomart_json = df_final_indo.to_dict(orient='records')

            non_indomart_json = final_table.to_dict(orient='records')

            indomart_main = item_creating(indomart_json, 'indomart_json_output')
            creating_plot('json_main',
                          indomart_main,
                          table_creating_command='(Processing_Date text NOT NULL, Type text NOT NULL, output_json JSONB)')
            mainlogging.info('preparaing rfm json')

            nonindomart_main = item_creating(non_indomart_json,
                                             'nonindomart_json_output')
            creating_plot('json_main',
                          nonindomart_main,
                          table_creating_command='(Processing_Date text NOT NULL, Type text NOT NULL,  output_json JSONB)')
            mainlogging.info('preparaing rfm json')

            df_rfm_output = df_rfm_output.astype(object).where(df_rfm_output.notnull(), None)

            json_input = df_rfm_output.to_dict(orient='records')
            item_rfm = item_creating(json_input, 'json_rfm')
            creating_plot('json_main',
                          item_rfm,
                          table_creating_command='(Processing_Date text NOT NULL, Type text NOT NULL,  output_json JSONB)')
            mainlogging.info('preparaing customer error json')
            customer_error = customer_error.astype(object).where(customer_error.notnull(), None)

            customer_error_json = customer_error[['customer_id',
                                                  master_list_cname,
                                                  master_list_city,
                                                  master_list_address,
                                                  'error_type']].to_dict(orient='records')
            customer_rfm = item_creating(customer_error_json, 'cusotmer_error_json')
            creating_plot('json_main',
                          customer_rfm,
                          table_creating_command='(Processing_Date text NOT NULL, Type text NOT NULL,  output_json JSONB)')

        except Exception as e:

            mainlogging.error(e)

        finally:
            mainlogging.info('sending emails')

            sending_emails(from_address=from_address,
                           user_name=user_name,
                           password=password,
                           receiving_address=receiving_address)

            mainlogging.info('removing files from directory')
            for filename in file_downloaded:
                os.unlink(downloadPath + filename)

            mainlogging.info("Sending logs to anyan.sun@datavlt.com through emial")
    sched.start()


if __name__ == '__main__':
        main()
