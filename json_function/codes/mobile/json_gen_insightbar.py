# -*- coding: utf-8 -*-
"""
Created on Mon Mar 23 09:47:32 2020

@author: anyan.sun
"""
from_six_indo = (df_final_indo['invoice_date'].max()\
                          -datetime.timedelta(6*365/12)).date().strftime('%Y-%m-%d')   
conn=create_conn()
cur = conn.cursor()

final_table_indo = pd.read_sql_query("SELECT * from final_indo where invoice_date >= '2019-08-08'", conn)

cur.close()
conn.close()

######### Insight Bar Location ###################

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

########### Insight Bar Companies #####
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

########################### mobile ########################


