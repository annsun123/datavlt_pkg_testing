# -*- coding: utf-8 -*-
"""
Created on Tue Mar 24 12:28:41 2020

@author: anyan.sun

"""
import calendar
import pandas as pd 
import numpy as np
import random
import datetime
from psycopg2.extras import Json
from json_function.codes.supporting_function import create_conn, creating_graph_json, color_variant

def json_main_mobile(df_mobile,table_type):
    
  
    if table_type=='non_indo':
        df_mobile = df_mobile[df_mobile['customer_name']!='Indomarco Prismatama, PT']
        df_mobile['city'] = df_mobile['city'].apply(lambda x: x.split(',')[0])
        df_mobile[['city','province', 'sku']] = df_mobile[['city','province', 'sku']].applymap(lambda x : x.title())
        df_mobile = df_mobile.astype(object).where(df_mobile.notnull(), None)
        df_mobile['province']=df_mobile['province'].replace('Nan', 'Unknown')
        df_mobile['customer_name']=df_mobile['customer_name'].apply(lambda x: x.split('(')[0])
        df_mobile = df_mobile.applymap(lambda x: x.strip() if type(x)==str else x)
    else:
        df_mobile = df_mobile[df_mobile['customer_name']!='Indomarco Prismatama, PT']
        df_mobile['city'] = df_mobile['city'].apply(lambda x: x.split(',')[0])
        df_mobile[['city','province', 'sku']] = df_mobile[['city','province', 'sku']].applymap(lambda x : x.title())
        df_mobile = df_mobile.astype(object).where(df_mobile.notnull(), None)
        df_mobile['province']=df_mobile['province'].replace('Nan', 'Unknown')
        df_mobile['customer_name']=df_mobile['customer_name'].apply(lambda x: x.split('-(')[0])
        df_mobile = df_mobile.applymap(lambda x: x.strip() if type(x)==str else x)
       
   
    colors_option = ['#00BD33', '#c5704b', '#BD009C', '#8B4513', '#8B8113']
              # [green, red, black, orange, brown, Olive]
    color_legend = {'Booster A': '#101214', 'Booster B': '#fa0729', 'Starter Deck':'#0068BD'}
                    
    df_mobile['product_name'] = df_mobile[['sku', 'series_num']].apply(lambda x: x['sku']+'; Series '+ str(x['series_num']), 1)
       
    stack_dic = {'Booster A': 2, 'Booster B': 3, 'Starter Deck':1}
    for i in df_mobile['sku'].unique():
        if i not in stack_dic.keys():
            stack_dic[i] = max(list(stack_dic.values()))+1
    
    color_dic = {}
    for sku in df_mobile['sku'].unique():
        color_var = {}
        index = 0
        if sku in color_legend.keys():
            base_color = color_legend[sku]
        else:
            color_legend[sku] =  colors_option[index]
            base_color = colors_option[index]
        series = sorted([int(i) for i in df_mobile[df_mobile['sku'] == sku]['series_num'].unique()],reverse=False)
        for series_num in range(len(series)):
           
            if series_num == 0:
                color_var[series[series_num]] = base_color
            else:
                if len(series) >= 5:
                    color = color_variant(base_color, brightness_offset=30*series_num)
                else:
                    color = color_variant(base_color, brightness_offset=50*series_num)
                color_var[series[series_num]] = color
        index += 1
        color_dic[sku] = color_var
        
    ################# mobile country json #############
    month_dict = {num:abb for num, abb in enumerate(calendar.month_abbr)}
    mobile_color_list=['#032762', '#064181','#0B5EA3', '#367DC6', '#609EE9', '#7DB0F0', '#B1D0F6'] 
    df_mobile['city'] = df_mobile['city'].apply(lambda x: x.split(',')[0])
    df_mobile[['city','province', 'sku']] = df_mobile[['city','province', 'sku']].applymap(lambda x : x.title())
    df_mobile = df_mobile.astype(object).where(df_mobile.notnull(), None)
    df_mobile['province']=df_mobile['province'].replace('Nan', 'Unknown')
    df_mobile['customer_name']=df_mobile['customer_name'].apply(lambda x: x.split('(')[0])
    df_mobile = df_mobile.applymap(lambda x: x.strip() if type(x)==str else x)
    
    list_array = df_mobile[['year','month']].drop_duplicates().values.tolist()
     
    df_month = df_mobile.groupby(['year', 'month'])['amount_million'].sum().round(2).reset_index()
    bar_json_country = {}
    bar_json_country['labels'] = [month_dict[x[-1]] for x  in list_array]
    
    bar_json_country['data'] = df_month['amount_million'].values.tolist()
    bar_json_country['backgroundColor'] = mobile_color_list[:len(df_month['amount_million'].values.tolist())]
    bar_json_country['graph_description']='mobile bar monthly country data'   
    
    
    ###################### mobile province json ###################### 
    
    list_array=df_mobile[['year','month']].drop_duplicates().values.tolist()
    for x in list_array: x.insert(0,'') 
    
    df=pd.DataFrame()
    for prov in df_mobile['province'].unique(): 
        for i in range(len(list_array)): list_array[i][0]=prov 
       
        df=df.append(list_array)
     
    df.columns = ['province', 'year','month']
    df = df.merge(df_mobile[['province', 'year','month','amount_million']], how='left', on=['province', 'year','month']).fillna(0)
    df_month = df.groupby(['province', 'year', 'month'])['amount_million'].sum().round(2).reset_index()
    bar_json_prov = {}
    bar_json_prov['labels']=[month_dict[x[-1]] for x  in list_array]
    dataset_list = []
    for prov in df_month['province'].unique():
        
        dataset_dic = {}
        dataset_dic['label'] = prov
        dataset_dic['data'] = df_month[df_month['province']==prov]['amount_million'].values.tolist()
        dataset_dic['backgroundColor'] = mobile_color_list[:len(df_month[df_month['province']==prov]['amount_million'].values.tolist())]
        dataset_list.append(dataset_dic)
        
    bar_json_prov['datasets'] = dataset_list
    bar_json_prov['graph_description']='mobile bar monthly province data'
    
    
    ################# mobile city json #############################
    
    list_array=df_mobile[['year','month']].drop_duplicates().values.tolist()
    for x in list_array: x.insert(0,'') 

    df=pd.DataFrame()
    for city in df_mobile['city'].unique(): 
        for i in range(len(list_array)): 
            list_array[i][0] = city 
        
        df=df.append(list_array)
     
    df.columns = ['city', 'year','month']
    df = df.merge(df_mobile[['city', 'year','month','amount_million']],\
                  how='left', on=['city', 'year','month']).fillna(0)
    df_month = df.groupby(['city', 'year', 'month'])['amount_million'].sum().round(2).reset_index()
    bar_json_city = {}
    bar_json_city['labels']=[month_dict[x[-1]] for x  in list_array]
    dataset_list = []
    for city in df_month['city'].unique():
        
        dataset_dic = {}
        dataset_dic['label'] = city
        dataset_dic['province'] = df_mobile[df_mobile['city']==city]['province'].iloc[0]
        dataset_dic['data'] = df_month[df_month['city']==city]['amount_million'].values.tolist()
        dataset_dic['backgroundColor'] = mobile_color_list[:len(df_month[df_month['city']==city]['amount_million'].values.tolist())]
        dataset_list.append(dataset_dic)
        
    bar_json_city['datasets'] = dataset_list
    bar_json_city['graph_description']='mobile bar monthly city data'
    
    
    
    ################# Companies ###########################
    
    list_array=df_mobile[['year','month']].drop_duplicates().values.tolist()
    for x in list_array: x.insert(0,'') 
    
    df=pd.DataFrame()
    for customer_name in df_mobile['customer_name'].unique(): 
        for i in range(len(list_array)): list_array[i][0]=customer_name 
       
        df=df.append(list_array)
     
    df.columns = ['customer_name', 'year','month']
    df = df.merge(df_mobile[['customer_name', 'year','month','amount_million']], how='left', on=['customer_name', 'year','month']).fillna(0)
    df_month = df.groupby(['customer_name', 'year', 'month'])['amount_million'].sum().round(2).reset_index()
    bar_json_comp = {}
    bar_json_comp['labels']=[month_dict[x[-1]] for x  in list_array]
    dataset_list = []
    for customer_name in df_month['customer_name'].unique():
        
        dataset_dic = {}
        dataset_dic['label'] = customer_name
        dataset_dic['city'] = df_mobile[df_mobile['customer_name']==customer_name]['city'].iloc[0]
        value = df_month[df_month['customer_name']==customer_name]['amount_million'].values.tolist()
        dataset_dic['data'] = value
        dataset_dic['backgroundColor'] = mobile_color_list[:len(value)]
        dataset_list.append(dataset_dic)
        
    bar_json_comp['datasets'] = dataset_list
    bar_json_comp['graph_description']='mobile bar monthly companies data'
    
    ####### Individual Company #########
    
    table = df_mobile.\
    groupby(['customer_name', 'year', 'month', 'product_name'])['amount_million'].sum().reset_index()#level=-1)
    table = table.round({'amount_million':2})           
    table[['year','month']]=table[['year','month']].applymap(lambda x: int(x))    
 
    table['date_label'] = table[['year','month']].apply(lambda x: datetime.date(x['year'],x['month'],1),1)
    
    
    min_date = df_mobile['invoice_date'].min()
    max_date = df_mobile['invoice_date'].max()
    
    df = pd.DataFrame()
    df_date = pd.DataFrame()
    idx=pd.date_range(min_date.strftime('%Y-%m-%d'), max_date.strftime('%Y-%m-%d'), freq='M')
    df_date['date']=idx
    df_date['year'] = df_date['date'].apply(lambda x: x.year)
    df_date['month'] = df_date['date'].apply(lambda x: x.month)
    df_date = df_date.drop('date', 1)
    
    for customer in table.customer_name.unique():
 
        dc_table = table[table['customer_name'] == customer]  
        df_new = df_date.merge(dc_table, how='left', on=['year','month']).fillna(0)
        df_new['customer_name']=customer
       
        df = df.append(df_new)
    ##########################
    max_num= df_mobile['series_num'].max()
    df['product_name']=df['product_name'].apply(lambda x: 'Booster A; Series '+str(max_num) if x==0 else x )
    df['backgroundColor'] = df['product_name'].apply(lambda x: color_dic[x.split(';')[0]][int(x.split('Series ')[1])])
    
    df['stack'] = df['product_name'].apply(lambda x: stack_dic[x.split(';')[0]])
    df['index'] = [i for i in range(len(df))]
 #   df['date_label']=df['month'].apply(lambda x: month_dict[x])
    df=df.rename(columns={'product_name':'label'})
    final_chart_json={}
    final_chart=[]
    
    for dc in df['customer_name'].unique():
        dc_dic = {} 
        dc_table = df[df['customer_name']==dc].reset_index(drop=True)
        dc_table['month_ref'] = dc_table[['year','month']].apply(lambda x: datetime.date(x['year'],x['month'],1),1)
        dc_dic['customer_name']=dc
        dc_dic['labels'] = [month_dict[x.month] for x in sorted(dc_table['month_ref'].unique())]
        dc_table['month_cat'] = dc_table['month'].astype('category').cat.codes
        dc_table['index'] = dc_table['index'].astype('category').cat.codes
        from scipy.sparse import csr_matrix
        spar_matrix=csr_matrix((dc_table['amount_million'], (dc_table['index'], dc_table['month_cat'])))
        table2 = pd.DataFrame(spar_matrix.todense(), \
                              columns=dc_table['month'].unique().tolist())
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
    final_chart_json['dataForProducts']=final_chart

      
    mobile_json_list = [bar_json_country, \
                        bar_json_prov, bar_json_city,\
                        bar_json_comp, final_chart_json]
    
    
    # insert into the table

    
    ######################  company 
    return mobile_json_list

