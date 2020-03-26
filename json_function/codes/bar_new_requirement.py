# -*- coding: utf-8 -*-
"""
Created on Thu Mar 26 12:07:42 2020

@author: anyan.sun
"""
from json_function.codes.supporting_function import creating_graph_json, color_variant, getDateRangeFromWeek
from_one_nonindo = (date_value\
             -datetime.timedelta(1*365/12)).strftime('%Y-%m-%d') 
         
from_three_nonindo = (date_value\
                  -datetime.timedelta(3*365/12)).strftime('%Y-%m-%d')    

conn=create_conn()
cur = conn.cursor()
final_table_nonindo = pd.read_sql_query("SELECT * from final_nonindo where invoice_date >= '2019-08-30'", conn)
cur.close()
conn.close()

stack_dic = {'Booster A': 2, 'Booster B': 3, 'Starter Deck':1}
for i in final_table_nonindo['sku'].unique():
    if i not in stack_dic.keys():
        stack_dic[i] = max(list(stack_dic.values()))+1
final_table_nonindo['product_name'] = final_table_nonindo[['sku', 'series_num']].apply(lambda x: x['sku']+'; Series '+ str(x['series_num']), 1)

table = final_table_nonindo.\
groupby(['customer_name', 'year', 'month', 'product_name',\
         'week_of_month'])['amount_million'].sum().reset_index()#level=-1)
table = table.round({'amount_million':2})           
table[['year','month','week_of_month']] = table[['year','month','week_of_month']].applymap(lambda x: int(x)) 
table['date_label'] = table[['year','month']].apply(lambda x: datetime.date(x['year'],x['month'],1),1)


min_date = final_table_nonindo['invoice_date'].min()
max_date = final_table_nonindo['invoice_date'].max()

df = pd.DataFrame()
df_date = pd.DataFrame()
idx=pd.date_range(min_date.strftime('%d-%m-%Y'), max_date.strftime('%d-%m-%Y'), freq='W')
df_date['date']=idx
df_date['year'] = df_date['date'].apply(lambda x: x.year)
df_date['week_of_month'] = df_date['date'].apply(lambda x: x.week)
df_date
df_date = df_date.drop('date', 1)

for customer in table.customer_name.unique():
 
    dc_table = table[table['customer_name'] == customer]  
    df_new = df_date.merge(dc_table, how='left', on=['year','week_of_month']).fillna(0)
    df_new['customer_name']=customer
   
    df = df.append(df_new)

df['beg_date']=df[['year','week_of_month']].apply(lambda x: getDateRangeFromWeek(x['year'],x['week_of_month'])[0],1)
df['end_date']=df[['year','week_of_month']].apply(lambda x: getDateRangeFromWeek(x['year'],x['week_of_month'])[1],1)
df['date_range']=df['beg_date'].astype(str)+' to '+ df['end_date'].astype(str)
max_num=final_table_nonindo['series_num'].max()
df['product_name']=df['product_name'].apply(lambda x: 'Booster A; Series '+str(max_num) if x==0 else x )
df['backgroundColor'] = df['product_name'].apply(lambda x: color_dic[x.split(';')[0]][int(x.split('Series ')[1])])

df['stack'] = df['product_name'].apply(lambda x: stack_dic[x.split(';')[0]])
df['index'] = [i for i in range(len(df))]
df['date_label']=df[['date_range','week_of_month']].apply(lambda x: x['date_range']+' '+'week '+ str(x['week_of_month']),1)
df=df.rename(columns={'product_name':'label'})
final_chart_json={}
final_chart=[]

for dc in df['customer_name'].unique():
    dc_dic={}
   
    dc_table = df[df['customer_name']==dc].reset_index(drop=True)
    dc_dic['customer_name']=dc

    dc_dic['labels'] = sorted(dc_table['date_label'].unique(),reverse=True)
    dc_table['beg_date_cat'] = dc_table['beg_date'].astype('category').cat.codes
    dc_table['index'] = dc_table['index'].astype('category').cat.codes
    from scipy.sparse import csr_matrix
    spar_matrix=csr_matrix((dc_table['amount_million'], (dc_table['index'], dc_table['beg_date_cat'])))
    table2 = pd.DataFrame(spar_matrix.todense(), \
                          columns=dc_table['date_range'].unique().tolist())
    table2 = table2[table2.columns[::-1]]
    table3 = dc_table.join(pd.DataFrame({'data':list(table2.values)}))
    table3 = table3.groupby(['label','backgroundColor','stack'])['data'].apply(lambda x: list(np.sum(x))).reset_index()
    #table3 = table3.groupby('product_name')[('backgroundColor','stack','data')].apply(lambda x: x.to_dict(orient='records')).reset_index(name="datasets")
    table3=table3.set_index('label')
    sku_list=table3.index.tolist()
    sku_list_new=[j for i, j in enumerate(sku_list) if 'Starter Deck' in j]+[elements for elements in sku_list if elements not in [j for i, j in enumerate(sku_list) if 'Starter Deck' in j]]
    table3 = table3.reindex(sku_list_new).reset_index()
    dc_dic['datasets'] = table3.to_dict(orient='records')
    final_chart.append(dc_dic)
final_chart_json['graph_description']='bar_com_weeks'
final_chart_json['data']=final_chart


with open ('new_bar_week.json', 'w') as file:
    json.dump(final_chart_json, file)
