import re
import datetime
import pandas as pd 
import numpy as np
# Self Packages 
from codes.otherFunction import google_geocode1
from codes.class_logging import logging_func
from codes.otherFunction import create_conn, table_check
process = logging_func('table_processing_log' , filepath='')
processing_logger = process.myLogger()


class processing:

    def __init__(self,
                 customer_indomaret,
                 customer_nonindomaret,
                 ctable_status,
                 customer_num_col,
                 master_list_city,
                 master_list_cname,
                 master_list_address,
                 master_list_province,
                 master_list_shoptype,
                 master_list_contact,
                 transaction_nonindo_file,qty_col,
                 description_col,
                 date_col,
                 amount_col,
                 booster_conversion,
                 starter_conversion,
                 cred):

        self.customer_indomaret = customer_indomaret
        self.customer_nonindomaret = customer_nonindomaret
        self.ctable_status = ctable_status
        self.customer_num_col = customer_num_col
        self.master_list_city = master_list_city
        self.master_list_cname = master_list_cname
        self.master_list_address = master_list_address
        self.master_list_province = master_list_province
        self.master_list_shoptype = master_list_shoptype
        self.master_list_contact = master_list_contact
        self.transaction_nonindo_file = transaction_nonindo_file
        self.qty_col = qty_col
        self.amount_col = amount_col
        self.description_col = description_col
        self.date_col = date_col  
        self.booster_conversion = booster_conversion
        self.starter_conversion = starter_conversion
        self.cred = cred


    def first_customer_table(self):

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
            processing_logger.info('table exists')   

            processing_logger.info('loading old latest customer table')

            command = 'select * from customer_table where updated_date = (select max(updated_date) from customer_table)'


            conn = create_conn()
            cur = conn.cursor()
            cur.execute(command)
            table = cur.fetchall()
            cur.close()
            curs = conn.cursor()

            curs.execute("Select * FROM customer_table LIMIT 0")
            colnames = [desc[0] for desc in curs.description]
            cur.close()
            conn.close()

            df_customer_old = pd.DataFrame(table, columns=colnames)
            processing_logger.info('old latest customer table loaded successfully')

            if self.ctable_status == 'not_uploaded':
                final_customer_table = df_customer_old.copy()
            else:

                try:

                    processing_logger.info('processing new customer table')

                    self.customer_indomaret['Customer Number'] = self.customer_indomaret[self.master_list_cname].apply(
                        lambda x: re.search(r'\w+[\d\-]\w+',x).group() 
                            if '(' in x else x
                    )

                    self.customer_indomaret['indo_info'] = 'indo'
                    self.customer_nonindomaret['indo_info'] = 'non_indo'

                    self.customer_indomaret.columns = self.customer_nonindomaret.columns
                    df_customer_new = self.customer_nonindomaret.append(
                        self.customer_indomaret, 1
                    )

                    #Capital all character in the table
                    for i in [self.master_list_city,
                              self.master_list_cname,
                              self.master_list_address,
                              self.master_list_contact,
                              self.master_list_shoptype,
                              self.master_list_province,
                              'ind_info']:

                        df_customer_new[i] = df_customer_new[i].str.upper()
                    df_customer_new = df_customer_new.rename(
                        columns = {self.customer_num_col: 'customer_id'}
                    )
                    processing_logger.info('new customer has columns: '+ ', '.join(df_customer_new.columns))
                    processing_logger.info('left merging new customer_table to old_customer table')
# merging the new customer table to the old customer table 
                    df_customer_new['customer_id'] = df_customer_new['customer_id'].apply(
                        lambda x: str(x)
                    )
                    df_customer=df_customer_new.merge(
                        df_customer_old,how='left', on='customer_id'
                    )

                    processing_logger.info('merging successfully')

                    processing_logger.info('filter out customer with none city or none address')

# electing all rows in the customer_indomrate table, and select rows with both Address and City information for indomarate as df_customer_api. [city==np.nan and address==np.nan are reported as error]

                    df_customer_api=df_customer[
                        ~(pd.isnull(df_customer[self.master_list_address]) | pd.isnull(df_customer[self.master_list_city])) &
                        (df_customer['indo_info'] == 'NON_INDO')
                    ].append(df_customer[df_customer['indo_info'] == 'INDO'])
                    df_customer_api['full_address'] = df_customer_api[
                        [self.master_list_address, self.master_list_city]
                    ].apply(lambda x: ', '.join(x.dropna()) + ', Indonesia', axis=1)
                    processing_logger.info('acquiring customer address')

# call api for df_customer_api where longtitude is null. [if the longtitue is not null which means it already has longtitude information, no need to call.]

                    table, error_address = google_geocode1(
                        df_customer_api[df_customer_api['longtitude'].isnull()],
                        'full_address',
                        'customer_id',
                        self.cred
                    )

# the output table does have all the longtitude and longtitude information. To mapping the table information to the df_customer_api table
                    processing_logger.info('updating customers address')
                    df_customer_api.loc[df_customer_api['longtitude'].isnull(), ['formal_address',
                                                                                 'longtitude',
                                                                                 'latitude']] = table[['Formatted Address',
                                                                                                       'Longitude',
                                                                                                       'Latitude']].values

                    processing_logger.info('preparing problematic table')
                    problematic_table = df_customer[(
                        pd.isnull(df_customer[self.master_list_address]) | pd.isnull(df_customer[self.master_list_city])) &
                              (df_customer['indo_info'] == 'NON_INDO'
                              )].append(df_customer.iloc[error_address])

# eporting the customer error  table the customer error is considered happening when table type is non indomarate and city or address values 
# is none or when calling geo_api, the first call is not able to get the value. 
# These information will be reported and adding to the main table as non-process version[no longittude and latitude values]
                    problematic_table['error_type'] ='missing city or address'
                    problematic_table.loc[error_address,
                                          'error_type'] = 'address and api calling require double confirmation'

                    processing_logger.info('prepare the final table')
                    processing_logger.info('merging the problematic table with api')
                    final_customer_table = df_customer_api.merge(problematic_table,
                                                                 how='outer').merge(table, how='outer')
                    final_customer_table = final_customer_table.drop_duplicates(
                        subset=['customer_id', 'Customer Name']
                    )

                    processing_logger.info('final table error_status update')

                    final_customer_table.loc[final_customer_table['error_type'].isnull(),
                                             'error_type'] = 'No_Error'
                    final_customer_table.loc[final_customer_table[self.master_list_city].isnull(),
                                             self.master_list_city] = 'CITY NA'

                    processing_logger.info('selecting columns')

                    final_customer_table, problematic_table = [x.drop('Unnamed: 0',1) 
                                                               if 'Unnamed: 0' in x 
                                                               else x for x in [final_customer_table,problematic_table]]

                    final_customer_table = final_customer_table[['customer_id',
                                                                 self.master_list_cname,
                                                                 self.master_list_address,
                                                                 'formal_address',
                                                                 self.master_list_city,
                                                                 self.master_list_province,
                                                                 self.master_list_shoptype,
                                                                 self.master_list_contact,
                                                                 'longtitude',
                                                                 'latitude',
                                                                 'error_type',
                                                                 'indo_info']]
                    final_customer_table['updated_date'] = datetime.date.today()
                    processing_logger.info('successfully process customer table')

                except IndexError as e:
                    processing_logger.error(e)

        else:
            processing_logger.info('table does not exists')
            if self.ctable_status == 'not_uploaded':
                processing_logger.info('the table is not uploaded')
            else:
                try:
                    processing_logger.info('capitablizing tables ') 
                    self.customer_indomaret['Customer Number']=self.customer_indomaret[self.master_list_cname].apply(
                        lambda x: re.search(r'\w+[\d\-]\w+', x).group() 
                        if '(' in x else x
                    )
                    self.customer_indomaret['indo_info'] = 'indo'
                    self.customer_nonindomaret['indo_info'] = 'non_indo'

                    self.customer_indomaret.columns = self.customer_nonindomaret.columns
                    df_customer = self.customer_nonindomaret.append(self.customer_indomaret, 1)

                    for i in [self.master_list_city,
                              self.master_list_cname,
                              self.master_list_address,
                              self.master_list_contact,
                              self.master_list_shoptype,
                              self.master_list_province,
                              'indo_info']:

                        df_customer[i] = df_customer[i].str.upper()

                    df_customer = df_customer.rename(columns = {self.customer_num_col: 'customer_id'})

                    processing_logger.info('selecting out non address/city columns') 

                    df_customer_api=df_customer[
                        ~(pd.isnull(df_customer[self.master_list_address]) | pd.isnull(df_customer[self.master_list_city])) & 
                        (df_customer['indo_info'] == 'NON_INDO')
                    ].append(df_customer[df_customer['indo_info'] == 'INDO'])
                    df_customer_api['full_address'] = df_customer_api[[self.master_list_address,
                                                                       self.master_list_city]].apply(
                        lambda x: ', '.join(x.dropna()) + ', Indonesia', axis=1
                    )  

                    processing_logger.info('searching api ') 
                    table,error_address = google_geocode1(df_customer_api, 
                                                          'full_address',
                                                          'customer_id', 
                                                          self.cred)

                    processing_logger.info('preparing error table ') 
                    problematic_table = df_customer[(pd.isnull(df_customer[self.master_list_address])| pd.isnull(df_customer[self.master_list_city])) &
                              (df_customer['indo_info'] == 'NON_INDO')].append(df_customer.iloc[error_address])

                    problematic_table['error_type'] = 'missing city or address'
                    problematic_table.loc[error_address,
                                          'error_type'] = 'address and api calling require double confirmation'

                    final_customer_table = df_customer_api.merge(problematic_table,
                                                                 how='outer').merge(table,how='outer')

                    final_customer_table.loc[final_customer_table['error_type'].isnull(),
                                             'error_type'] = 'No_Error'

                    final_customer_table, problematic_table = [x.drop('Unnamed: 0',1)
                                                               if 'Unnamed: 0' in x
                                                               else x for x in [final_customer_table, problematic_table]]

                    final_customer_table.loc[final_customer_table[self.master_list_city].isnull(),
                                             self.master_list_city] = 'CITY NA'

                    final_customer_table = final_customer_table[['customer_id',
                                                                 self.master_list_cname,
                                                                 self.master_list_address,
                                                                 'Formatted Address',
                                                                 self.master_list_city,
                                                                 self.master_list_province,
                                                                 self.master_list_shoptype,
                                                                 self.master_list_contact,
                                                                 'Longitude',
                                                                 'Latitude',
                                                                 'error_type', 
                                                                 'indo_info']]

                    final_customer_table.columns = ['customer_id',
                                                    self.master_list_cname,
                                                    self.master_list_address,
                                                    'formal_address',
                                                    self.master_list_city,
                                                    self.master_list_province,
                                                    self.master_list_shoptype,
                                                    self.master_list_contact,
                                                    'longtitude',
                                                    'latitude',
                                                    'error_type', 
                                                    'indo_info']
                    final_customer_table['updated_date'] = datetime.date.today()

                    processing_logger.info('processing customer table successfully')
                except IndexError as e:
                    processing_logger.error(e)

        return final_customer_table, problematic_table


    def processing_history_nonindo(self):

# transaction_file_name,qty_col, description_col, date_col,booster_conversion=720, starter_conversion=120):
        try:
            df = pd.read_excel(self.transaction_nonindo_file)

            row_skip = df[df.isin([self.date_col]).any(axis=1)].index[0] + 1
            df_daily = pd.ExcelFile(self.transaction_nonindo_file)
            df_daily = df_daily.parse('Sheet1', skiprows=row_skip)
            day_fill = df_daily.drop(df_daily[df_daily[self.customer_num_col] == 'Total'].index)
            day_fill[self.date_col] = day_fill[self.date_col].fillna(method='ffill')

            day_fill_test1 = day_fill.dropna(axis=1, how='all', inplace=False)

            index_list = np.where(~day_fill_test1.iloc[0].isna())[0]

            sku_indval = [(ind, value) for ind, value in enumerate(day_fill_test1.columns[index_list])
                          if 'Unnamed' not in value]

            for i in range(len(sku_indval)):
                day_fill_test1.insert(int(index_list[sku_indval[i][0]]) + i,
                                      'sku_name' + str(i), sku_indval[i][1])
            index_list2 = np.where(~day_fill_test1.iloc[0].isna())[0]
            day_fill_test1 = day_fill_test1.drop(day_fill_test1.index[0])
            day_fill_test1 = day_fill_test1.drop(day_fill_test1.index[-1])
            
# df_final_test.append(day_fill_test1[day_fill_test1.columns[index_list2[i:i+3]]])
            df_final_test = pd.DataFrame()
            for i in range(0, len(index_list2), 3):

                df_final_list_test = []
                df_final_list_test.append(day_fill_test1.iloc[:, :3])
                df_final_list_test.append(
                    day_fill_test1[day_fill_test1.columns[index_list2[i:i + 3]]]
                )
                data = pd.concat(df_final_list_test, axis=1)
                data.columns = ['Date',
                                'customer_id',
                                'customer_name',
                                'sku_type',
                                'qty(MC)',
                                'amount']
                df_final_test = df_final_test.append(data)

            df_final_test['qty(MC)'] = df_final_test['qty(MC)'].apply(lambda x : np.nan
                                                                      if x =='-' else x)
            df_final_test['amount'] = df_final_test['amount'].apply(lambda x : np.nan
                                                                    if x =='-' else x)
            df_final_test=df_final_test.dropna(subset=['qty(MC)', 'amount'])

# sku name Processing
            df_final_test['sku'] = df_final_test['sku_type'].apply(lambda x: 'BOOSTER A'
                                                                   if ' A' in x.upper()
                                                                   else('BOOSTER B'if ' B'  in x.upper()
                                                                   else ('STARTER DECK' if 'STARTER' in x.upper() else x)))
            df_final_test['series'] = df_final_test['sku_type'].apply(lambda x: re.search(r'\d',x).group(0)
                                                                      if bool(re.search(r'\d',x))
                                                                      else '1')
            processing_logger.info('sereis contains values: ' + ', '.join(df_final_test['series'].unique()))
            df_final_test['qty(pakcs)'] = df_final_test.apply(lambda x: x['qty(MC)'] * self.booster_conversion
                                                              if x['sku']!='STARTER DECK'
                                                              else x['qty(MC)'] * self.starter_conversion,
                                                              axis=1)
            processing_logger.info('table contains columns: ' + ', '.join(df_final_test.columns))
            processing_logger.info('table pre-processed successfully')
            return df_final_test

        except (IndexError, KeyError) as e:
            processing_logger.error('Please checking description name and sku names or Table format has been changed')


def processing_history_indo(transaction_indo_file,booster_conversion,starter_conversion):

# transaction_file_name,qty_col, description_col, date_col,booster_conversion=720, starter_conversion=120):
    try:
# transaction_file_name=downloadPath+transaction_nonindo
        df = pd.read_excel(transaction_indo_file)
        df.dropna(axis=1, how='all', inplace=True)
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

# find start and end rows for creating different dataframes to merge 
        start_row = df[df.isin(['Invoice Date']).any(axis=1)].index
        end_row = df[df.isin(['Total']).any(axis=1)].index

        df_final = pd.DataFrame()
        for x in range(len(start_row)):
            df_new = df.loc[start_row[x]:end_row[x], :].copy()
            df_new.dropna(axis=1, how='all', inplace=True)
# setting colnames - invoie date
            row_skip = df_new[df_new.isin(['Invoice Date', 'Amount', 'Total']).any(axis=1)].index.to_list() 
            invoice_date = df_new.loc[row_skip[0],:] == 'Invoice Date'
            df_new.rename(columns={invoice_date[invoice_date==True].index[0]: 'Invoice Date'},
                          inplace=True)
# setting rest of the cols 
            col_level_2 = df.iloc[row_skip[0]+1, :] 
            old_col = col_level_2[~col_level_2.isna()].index.tolist()
            new_col = col_level_2[~col_level_2.isna()].values.tolist()
            df_new.rename(columns=dict(zip(old_col, new_col)), inplace=True)
# Misc steps and cleaning 
# finding text to put in prod_desc
            lst_sku = df_new.loc[row_skip[0], 'Sales Qty'].to_list()
# finding index of Amount to add new cols
            lst_append = [idx for idx, val in enumerate(df_new.columns.get_loc("Amount"))
                          if val == True]
# deleting rows with texts in it
            df_new.drop(row_skip, axis=0, inplace=True)
# fillig date col with forward fill
            df_new.loc[:, 'Invoice Date']= df_new.loc[:,'Invoice Date'].ffill().astype(str)

# adding new columns and melting 
            for i in range(len(lst_append)):
                df_new_col  = df_new.iloc[:, [0,1, lst_append[i]-1, lst_append[i]]].copy()
                df_new_col['prod_desc'] = lst_sku[i]
                df_final = df_final.append(df_new_col, ignore_index=True)

# finding Series and SKU from prod_desc
# removing numeric value-(23345
        df_final['prod_desc'] =  [x[0].strip().upper()
                                  for x in df_final['prod_desc'].str.split('-')]
        df_final['SKU'] = [' '.join([x.split()[1], x.split()[6]]) 
                           if 'BOOSTER' in x
                           else 'STARTER DECK' 
                           for x in  df_final['prod_desc']]
        df_final['Series'] = [x.split()[len(x.split())-2]
                              if 'SERIES' in x 
                              else 1 for x in  df_final['prod_desc']]

# removign blank rows
        df_final.fillna(0, inplace=True)
        df_final.loc[:, ['Sales Qty', 'Amount']] = df_final[['Sales Qty', 'Amount']].replace('-', 0)
        df_final=df_final.rename(columns={'Sales Qty': 'qty(MC)'})

        df_final.drop(df_final[(df_final['qty(MC)'] == 0) & (df_final['Amount'] == 0)].index, axis=0, inplace=True)
        df_final['qty(pakcs)'] = df_final.apply(lambda x: x['qty(MC)'] * booster_conversion 
                                                if x['SKU'] != 'STARTER DECK' else x['qty(MC)'] * starter_conversion, axis=1)
        df_final.drop('prod_desc', axis=1, inplace=True)
        del(df_new, df_new_col)  
        return df_final 

    except (IndexError, KeyError) as e:
        processing_logger.error('Please checking description name and sku names or Table format has been changed')
