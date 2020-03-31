# -*- coding: utf-8 -*-
"""
Created on Thu Feb 27 14:50:03 2020

@author: anyan.sun
"""
from json_function.codes.supporting_function import create_conn
from codes.class_rfm import createRFM_dataset, clustering, defineCluster
import pandas as pd 
import datetime
from psycopg2.extras import Json

def creating_json_rfm():
    
    conn=create_conn()
    cur = conn.cursor()
    
    final_table = pd.read_sql_query("SELECT * from final_nonindo", conn)
    
    cur.close()
    conn.close()
    
    final_table['customer_name'] = final_table['customer_name'].apply(lambda x: x.split('(')[0])
    final_table = final_table.applymap(lambda x: x.strip() if type(x)==str else x)
    
    raw_ds = final_table.copy()
    
    if 'Unnamed: 0' in raw_ds:
        raw_ds.drop('Unnamed: 0', axis=1, inplace=True)
    
    # raw_ds['amount']=raw_ds.apply(lambda x: x['qty_packs']*15000000 if x['sku']=='STARTER DECK' else  x['qty_packs']*30000000, axis=1)
    raw_ds['amount']=raw_ds['amount']/1000000
    raw_ds['invoice_date'] = pd.to_datetime(raw_ds['invoice_date'])
    raw_ds['Avg_order_gap'] = (raw_ds['invoice_date'].max() - raw_ds['invoice_date']).dt.days
        
    
    
    str_cols = ['Frequency', 'Monetary', 'Avg_order_gap', 'Recency']
    
    df_rfm_overall = createRFM_dataset(raw_ds)
    df_rfm_overall['cluster'] = clustering(df_rfm_overall, str_cols, "algo") # gives best 
    #str_cluter_def = ['Star','Cash Cows', 'Question Marks', 'Dogs']
    str_cluter_def = ['The Star Performer','High Spender', 'Loyal Spender', 'Low Spender']
    df_rfm_overall = defineCluster(df_rfm_overall, str_cluter_def)
       
    df_rfm_output = final_table.merge(df_rfm_overall[['customer_id','cluster','Recency']],how='left', on='customer_id')
    df_rfm_output['invoice_date'] = pd.to_datetime(df_rfm_output['invoice_date'])
    
    df_rfm_output['Avg_order_gap'] =  (df_rfm_output['invoice_date'].max() - df_rfm_output['invoice_date']).dt.days
    df_rfm_overall=df_rfm_overall.rename(columns={'Avg_order_gap':'Time(Light)', 'Monetary':'Sales',\
                         'Recency':'Time(Dark)', 'Frequency':'Purchase'})
                         
    df_rfm_city = df_rfm_overall.groupby(['cluster','city'])\
    .agg({'Time(Light)': 'mean', 'Sales': 'sum', 'Time(Dark)': 'mean', \
          'Purchase': 'mean'}).reset_index()
    df_rfm_city = df_rfm_city.round({'Time(Dark)':0,'Time(Light)':0,'Purchase':0,'Sales':2})
       
                        
    
    df_rfm_output = df_rfm_output.groupby(['cluster','series_num', 'sku']).\
    agg({'Recency': 'mean',  'Avg_order_gap': 'mean', \
          'qty_packs': 'count', 'amount_million': 'sum'}).reset_index()
    #df_rfm_output['invoice_date'] = df_rfm_output['invoice_date'].astype(str)
    df_rfm_output = df_rfm_output.round({'Recency':0,'Avg_order_gap':0,'qty_packs':0,'amount_million':2})
       
    df_rfm_cn = df_rfm_overall[['cluster', 'customer_name', 'city', 'Time(Dark)', \
                       'Sales', 'Time(Light)', 'Purchase']]
    df_rfm_cn = df_rfm_cn.round({'Sales':2})
    
       
    df_rfm_output = df_rfm_output.astype(object).where(df_rfm_output.notnull(),None)
    
    json_input_series = df_rfm_output.to_dict(orient='records')## for series
    json_input_series.insert(0, {'graph_description': 'rfm filtering by series'})
    json_input_city = df_rfm_city.to_dict(orient='records') ## for city
    json_input_cn = df_rfm_cn.to_dict(orient='records') ## for customer 
    
    insight_bar_color ={'The Star Performer': '#5D9CEC', 'The Star Performer_light': '#C5E0FF', \
                    'High Spender': '#ED5565', 'High Spender_light': '#FFCFD5',\
                    'Loyal Spender': '#FF9B21', 'Loyal Spender_light': '#FFDFB6',\
                    'Low Spender': '#14404B', 'Low Spender_light': '#CFCFCF'}

    output_list=[]
    for cluster in df_rfm_city['cluster'].unique():
        data_json = {}
        data_json['cluster'] = cluster 
        city_lable = df_rfm_city[df_rfm_city['cluster'] == cluster]['city'].unique().tolist()
        datasets_list = []
        for measure_type in df_rfm_city.columns[-4:]:
            
            datasets = {}
            datasets['desc0'] = measure_type
           
            datasets['label'] = city_lable
            datasets['data']= df_rfm_city[df_rfm_city['cluster'] == cluster][measure_type].values.tolist()
            if measure_type == 'Time(Light)':
                datasets['backgroundColor'] = insight_bar_color[cluster+'_light']
            else:
                datasets['backgroundColor'] = insight_bar_color[cluster]
            datasets_list.append(datasets)
        data_json['datasets']=datasets_list
        output_list.append(data_json)
        
    output_json_location={}
    output_json_location['Filter'] = 'location'
    output_json_location['graph_description'] = 'insight_location'
    output_json_location['output'] = output_list
    
    ########### Insight Bar Companies ########## 
    
    output_list=[]
    for cluster in df_rfm_cn['cluster'].unique():
        data_json = {}
        data_json['cluster'] = cluster 
        df_com = df_rfm_cn[df_rfm_cn['cluster'] == cluster]
        com_lable = df_com['customer_name'].values.tolist()
        datasets_list = []
        for measure_type in df_rfm_cn.columns[-4:]:
            
            datasets = {}
            datasets['desc0'] = measure_type
            datasets['label'] = com_lable
           # com_lable = df_com[df_com[measure_type]==['customer_name'].unique().tolist()
            datasets['data']= df_com[measure_type].values.tolist()
            if measure_type == 'Time(Light)':
                datasets['backgroundColor'] = insight_bar_color[cluster+'_light']
            else:
                datasets['backgroundColor'] = insight_bar_color[cluster]
            datasets_list.append(datasets)
        data_json['datasets']=datasets_list
        output_list.append(data_json)
        
        
    output_json_com={}
    output_json_com['Filter'] = 'Company'
    output_json_com['graph_description'] = 'Insight_Company'
    output_json_com['output'] = output_list
    
    
    ########################  Insight Bar SERIES #########################
    output_list=[]
    df_rfm_output.columns = ['cluster', 'series_num', 'sku', \
                             'Time(Dark)','Time(Light)', 'Purchase',\
                           'Sales']
    df_rfm_output['sku_series'] = df_rfm_output[['sku','series_num']].apply(lambda x: x['sku']+'; Series '+ str(x['series_num']), 1)
         
    
    for cluster in df_rfm_output['cluster'].unique():
        data_json = {}
        data_json['cluster'] = cluster 
        city_lable = df_rfm_output[df_rfm_output['cluster'] == cluster]['sku_series'].unique().tolist()
        datasets_list = []
        for measure_type in df_rfm_output.columns[-5:-1]:
            
            datasets = {}
            datasets['desc0'] = measure_type
            datasets['label'] = city_lable
            datasets['data']= df_rfm_output[df_rfm_output['cluster'] == cluster][measure_type].values.tolist()
            if measure_type == 'Time(Light)':
                datasets['backgroundColor'] = insight_bar_color[cluster+'_light']
            else:
                datasets['backgroundColor'] = insight_bar_color[cluster]
            datasets_list.append(datasets)
        data_json['datasets']=datasets_list
        output_list.append(data_json)
        
        
    output_json_seri={}
    output_json_seri['Filter'] = 'sku_series'
    output_json_seri['graph_description'] = 'insight_series'
    output_json_seri['output'] = output_list
    
    ############## Each City as Filter #################
    # filter by each city's name 
    insight_lct_cn={}
    insight_lct_cn_list=[]
    
    for city in df_rfm_cn['city'].unique():    
        output_json={}
        output_json['Filter'] = city
       # output_json['data'] = df_rfm_cn[df_rfm_cn['city']==city].to_dict(orient='records')
        df_city = df_rfm_cn[df_rfm_cn['city']==city]
        output_list=[]
        for cluster in df_city['cluster'].unique():
            data_json = {}
            data_json['cluster'] = cluster 
            com_lable = df_city[df_city['cluster'] == cluster]['customer_name'].unique().tolist()
            datasets_list = []
            for measure_type in df_city.columns[-4:]:
                
                datasets = {}
                datasets['desc0'] = measure_type
                datasets['label'] = com_lable
                datasets['data']= df_city[df_city['cluster'] == cluster][measure_type].values.tolist()
                if measure_type == 'Time(Light)':
                    datasets['backgroundColor'] = insight_bar_color[cluster+'_light']
                else:
                    datasets['backgroundColor'] = insight_bar_color[cluster]
                datasets_list.append(datasets)
            data_json['datasets']=datasets_list
            output_list.append(data_json)
            
            
        output_json['data'] = output_list
        insight_lct_cn_list.append(output_json)
        
    #overall_list.append(output_json_seri)
    insight_lct_cn['graph_description'] = 'insight_city_filter'
    insight_lct_cn['output'] = insight_lct_cn_list
        
    
    to_date = ''
    from_date = ''
    rfm_json_table = []
    for (description, json_output) in [('Insight_location',output_json_location),\
                                ('Insight_Company', output_json_com),\
                                ('insight_series', output_json_seri),\
                                ('insight_city_filter', insight_lct_cn)]:
     
        rfm_json_table.append ([description, 'rfm', 'NA',\
                                 to_date, from_date, datetime.date.today(), Json(json_output)])
        
    insight_mobile_table = ['insight_mobile_companies', 'rfm', 'NA',\
                                 to_date, from_date, datetime.date.today(), Json(output_json_com)]
    
    return rfm_json_table, insight_mobile_table                         