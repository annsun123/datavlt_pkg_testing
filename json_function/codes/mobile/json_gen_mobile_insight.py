# -*- coding: utf-8 -*-
"""
Created on Tue Mar 24 14:52:17 2020

@author: anyan.sun
"""

def mobile_insight_json():
    
    final_table['customer_name']=final_table['customer_name'].apply(lambda x: x.split('(')[0])
    final_table = final_table.applymap(lambda x: x.strip() if type(x)==str else x)
    
    raw_ds = final_table.copy()
    
    if 'Unnamed: 0' in raw_ds:
        raw_ds.drop('Unnamed: 0', axis=1, inplace=True)
    
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
        
    return output_json_com