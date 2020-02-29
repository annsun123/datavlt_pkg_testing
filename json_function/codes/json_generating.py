from json_function.codes.supporting_function import creating_graph_json, color_variant, getDateRangeFromWeek
from json_function.codes.logging import logging_func
import pandas as pd
import numpy as np
from numpy import random
import random
import calendar
import datetime
import time

json_function=logging_func('jsongeneration_log',filepath='/')
jgntlogger = json_function.myLogger()

def creating_json_nonindo(final_table_nonindo):
    
    final_table_nonindo = final_table_nonindo[final_table_nonindo['customer_name']!='Indomarco Prismatama, PT']
    final_table_nonindo['city'] = final_table_nonindo['city'].apply(lambda x: x.split(',')[0])
    final_table_nonindo[['city','province', 'sku']] = final_table_nonindo[['city','province', 'sku']].applymap(lambda x : x.title())
    final_table_nonindo = final_table_nonindo.astype(object).where(final_table_nonindo.notnull(), None)
    final_table_nonindo['province']=final_table_nonindo['province'].replace('Nan', 'Unknown')
    final_table_nonindo['customer_name']=final_table_nonindo['customer_name'].apply(lambda x: x.split('(')[0])
    final_table_nonindo = final_table_nonindo.applymap(lambda x: x.strip() if type(x)==str else x)
    
    color_list=['#ED5565', '#006B72', '#FF8000', '#142F4B', '#881E00',\
            '#44B5BC', '#FFBF41', '#0669B4', '#CC4D8B', '#00A37B',\
            '#A1B746', '#693E00', '#005700', '#76079C', '#5D9CEC', \
            '#6C6DA3']
            
    colors_option = ['#00BD33', '#c5704b', '#BD009C', '#8B4513', '#8B8113']
              # [green, red, black, orange, brown, Olive]
    color_legend = {'Booster A': '#101214', 'Booster B': '#fa0729', 'Starter Deck':'#0068BD'}
    
    color_dic = {}
    for sku in final_table_nonindo['sku'].unique():
        color_var = {}
        index = 0
        if sku in color_legend.keys():
            base_color = color_legend[sku]
        else:
            color_legend[sku] =  colors_option[index]
            base_color = colors_option[index]
        series = sorted([int(i) for i in final_table_nonindo[final_table_nonindo['sku'] == sku]['series_num'].unique()],reverse=False)
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
        
        
    final_table_nonindo['product_name'] = final_table_nonindo[['sku', 'series_num']].apply(lambda x: x['sku']+'; Series '+ str(x['series_num']), 1)
     
    # top performing location
    topltn_df = final_table_nonindo[['city','qty_packs','amount_million', \
                                  'customer_id']].groupby('city').\
                                  agg({'qty_packs':'sum', 'amount_million':'sum'}).\
                                  reset_index()
    topltn_df=topltn_df.round({'amount_million':2, 'qty_packs':0})
    topltn_df = topltn_df.astype(object).where(topltn_df.notnull(), None)                           
    topltn_json = creating_graph_json(topltn_df, \
                                      graph_description = 'top_performing_location', \
                                      filter_link = 'NA')

    # top performing companies 
    topcomp_df = final_table_nonindo[['customer_name', 'qty_packs','amount_million', \
                                  'customer_id']].groupby('customer_name').\
                                  agg({'qty_packs':'sum', 'amount_million':'sum'}).\
                                  reset_index()
    topcomp_df = topcomp_df.round({'amount_million':2, 'qty_packs':0})                              
    topcomp_df = topcomp_df.astype(object).where(topcomp_df.notnull(), None)                              
    topcomp_json = creating_graph_json(topcomp_df, \
                                     graph_description = 'top_performing_companies', \
                                     filter_link = 'NA')
                  
                                
    # top performing products  
    
    topprodct_df = final_table_nonindo[['product_name', 'qty_packs', 'amount_million', \
                                  'customer_id']].groupby('product_name').\
                                  agg({'qty_packs': 'sum', 'amount_million': 'sum'}). \
                                  reset_index()
    topprodct_df = topprodct_df.round({'amount_million':2, 'qty_packs':0})                                
    topprodct_df = topprodct_df.astype(object).where(topprodct_df.notnull(), None)                                 
    topprodct_json = creating_graph_json(topprodct_df,\
                                         graph_description = 'top_performing_product', \
                                         filter_link = 'NA')
                      
    # creating json for all different graphs 
    geochart_prov_df = final_table_nonindo[['province', 'qty_packs', 'amount_million']].groupby('province').\
                                  agg({'qty_packs': 'sum', 'amount_million': 'sum'}). \
                                  reset_index()
    geochart_prov_df = geochart_prov_df.round({'amount_million':2, 'qty_packs':0})    
    geochart_prov_df = geochart_prov_df.sort_values(by='province')
    np.random.seed(123)
    for index,prov in enumerate(geochart_prov_df['province'].unique()):  
        geochart_prov_df.loc[geochart_prov_df['province'] == prov, 'province_color'] = color_list[index]
                  
    geochart_prov_json = {}                                 
    contry_revenue = geochart_prov_df['amount_million'].sum()
    geochart_prov_df['percentage'] = (geochart_prov_df['amount_million'] / contry_revenue)*100
    geochart_prov_df['percentage'] = [round(i,2) for i in geochart_prov_df.percentage.values.tolist()]
    geochart_prov_json['total_revenue'] = round(contry_revenue, 2)
    geochart_prov_df = geochart_prov_df.astype(object).where(geochart_prov_df.notnull(),None)
    value = geochart_prov_df.to_dict(orient='records')
    geochart_prov_json['output'] = value    
    geochart_prov_json['graph_description'] = 'all provinces within city'
                                                 
    # geo-chart on level of all companies in one province
    
    geochart_citi_df = final_table_nonindo[['province', 'city', 'qty_packs', 'amount_million']].groupby(['province', 'city']).\
                                  agg({'qty_packs': 'sum', 'amount_million': 'sum'}). \
                                  reset_index(level=1)
    geochart_citi_df = geochart_citi_df.round({'amount_million':2, 'qty_packs':0})                               
    #total revenue for each province
         
              
    table = final_table_nonindo.groupby('province')['amount_million'].sum().round(2).to_frame().rename(columns={'amount_million': 'province_total_amount_million'})
    
    geochart_citi_df['city_color']=0           
   
    for i in table.index:  
        geochart_citi_df.loc[i,  'city_percentage']= (geochart_citi_df.loc[i, 'amount_million']/table.loc[i,'province_total_amount_million'])*100
        length = [1 if type(geochart_citi_df.loc[i]['city'])==str else len(geochart_citi_df.loc[i])][0]
        if length==1:
            geochart_citi_df.loc[i, 'city_color']=random.sample(color_list,length)[0]
        else:
            geochart_citi_df.loc[i, 'city_color']=random.sample(color_list,length)
    geochart_citi_df['city_percentage']=[round(i,2) for i in geochart_citi_df.city_percentage.values.tolist()]
    
    geochart_citi_df['longtitude'], geochart_citi_df['latitude']=[np.nan, np.nan]
    
    for i in geochart_citi_df['city'].unique():
        table_x = final_table_nonindo[final_table_nonindo['city']==i]
        try:
            geochart_citi_df.loc[geochart_citi_df['city'] == i, ['longtitude', 'latitude']] = list(table_x[~table_x['latitude'].isnull()][['longtitude', 'latitude']].values[0])
        except:
            geochart_citi_df.loc[geochart_citi_df['city'] == i, ['longtitude', 'latitude']] = [np.nan, np.nan]
            
    geochart_citi_df = geochart_citi_df.reset_index().merge(table.reset_index(), how='left', on='province')
    geochart_citi_df['size_category']=pd.qcut(geochart_citi_df['amount_million'],4, labels=[i for i in range(4)])
  
    geochart_citi_df = geochart_citi_df.astype(object).where(geochart_citi_df.notnull(), None) 
    geochart_citi_json = creating_graph_json(geochart_citi_df,\
                                           graph_description = 'map chart all citie at each province level', \
                                           filter_link = 'None')   

    
    #  geo graph specific company level--total
    #getting cities geo coordinates 
    table=geochart_citi_df.groupby(['province', 'city'])['amount_million'].sum().to_frame().rename(columns={'amount_million': 'city_total_amount_million'}).round(2)
    
    geochart_comp_df = final_table_nonindo[['province', 'city', 'customer_name', 'qty_packs', 'amount_million']].groupby(['province', 'city', 'customer_name']).\
                                  agg({'qty_packs': 'sum', 'amount_million': 'sum'}). \
                                  reset_index(level=2)
    geochart_comp_df = geochart_comp_df.round({'amount_million':2, 'qty_packs':0})                               
    geochart_comp_df['color_category']=pd.qcut(geochart_comp_df['amount_million'],4, labels=[3,2,1,0])   
    geochart_comp_df['size_category']=pd.qcut(geochart_comp_df['amount_million'],4, labels=[0,1,2,3])  
    geochart_comp_df['company_percentage']=0
    for i in table.index:
        percentage=[round(value,2) for value in \
        (geochart_comp_df.loc[i, 'amount_million']/table.loc[i,'city_total_amount_million']*100).tolist()]   
        
        geochart_comp_df.loc[i,  'company_percentage']= percentage
    
    geochart_comp_df['city_color']=0
    for index,value in enumerate(geochart_comp_df.index):
        
        
        color =geochart_citi_df.loc[(geochart_citi_df['city']==geochart_comp_df.index[index][1])&\
        (geochart_citi_df['province']==geochart_comp_df.index[index][0]),'city_color'].values[0]
        geochart_comp_df.loc[value,'city_color'] = [color for time in range(len(geochart_comp_df.loc[value,'city_color']))]
  
    
    geochart_comp_df['company_color']=geochart_comp_df[['city_color','color_category']].apply(lambda x: color_variant(x['city_color'], brightness_offset=20*x['color_category']) if x['color_category'] !=0 else x['city_color'],1)    
    company_geo_dic={}
    for i in final_table_nonindo['customer_name'].unique():
        company_geo_dic[i] = list(final_table_nonindo[final_table_nonindo['customer_name'] == i].iloc[0][['longtitude', 'latitude']].values)
    geochart_comp_df['longtitude'] = geochart_comp_df['customer_name'].apply(lambda x: company_geo_dic[x][0])
    geochart_comp_df['latitude'] = geochart_comp_df['customer_name'].apply(lambda x: company_geo_dic[x][1])    
    geochart_comp_df=geochart_comp_df.drop(['color_category'],1)
    geochart_comp_df=geochart_comp_df.reset_index()
    geochart_comp_df = geochart_comp_df.astype(object).where(geochart_comp_df.notnull(), None) 
    geochart_comp_json = creating_graph_json(geochart_comp_df,\
                                           graph_description = 'map chart all companies at each city level', \
                                           filter_link = 'None')   

    
      
    ### doughnut charts 
    table = final_table_nonindo[['province',\
                         'city', 'customer_name', 'qty_packs', \
                         'amount_million', 'sku', 'series_num']].\
                         groupby(['province','city', 'customer_name','sku', 'series_num']).\
                          agg({'qty_packs': 'sum', 'amount_million': 'sum'}).reset_index()
    table = table.round({'amount_million':2, 'qty_packs':0})                        
    table['series_num'] = table['series_num'].apply(lambda x: 'series ' +str(x))
    table['backgroundColor'] = table['sku'].apply(lambda x: color_legend[x])
    table = table.merge(table.groupby(['province','city','customer_name'])['amount_million'].sum().round(2).reset_index().rename(columns={'amount_million':'comp_total_rev'}), how='left', on=['province','city','customer_name'])
    table = table.merge(table.groupby(['province','city','customer_name','sku'])['amount_million'].sum().round(2).reset_index().rename(columns={'amount_million':'sku_total_amount_million'}), how='left', on=['province','city','customer_name', 'sku'])
    
    table['sku_percentage'] = [round(value,2) for value in (table['sku_total_amount_million'] / table['comp_total_rev']*100).tolist()]
    table['series_percentage'] = [round(value,2) for value in (table['amount_million'] / table['sku_total_amount_million']*100).tolist()]
    table = table.rename(columns={'amount_million':'series_total_amount_million'})
    table = table[['province', 'city', 'customer_name', 'sku', 'series_num', 'qty_packs',\
                    'backgroundColor', 'comp_total_rev', 'sku_total_amount_million', 'sku_percentage',\
                   'series_total_amount_million','series_percentage' ]]

    table = table.astype(object).where(table.notnull(),None)  
    
    json_df2 = table.groupby(['province','city','customer_name','sku','backgroundColor',\
                              'comp_total_rev','sku_total_amount_million',\
                              'sku_percentage'])[['series_num','qty_packs', 'series_total_amount_million','series_percentage']].\
    apply(lambda x: x.to_dict(orient='records')).reset_index(name="data").\
    groupby(['province', 'city', 'customer_name'])[['sku','sku_total_amount_million', 'sku_percentage','backgroundColor', 'data']].\
    apply(lambda x: x.to_dict(orient='records')).reset_index(name="series_level")
    
    
    doughnut_json_comp={}
    doughnut_ls_comp=[]
    for i in table.customer_name.unique():
        
        customer_json={}
        customer_json['company_name'] = i
        customer_table = table[table['customer_name'] == i]
        customer_json['comp_total_value'] = customer_table['comp_total_rev'].iloc[0]
        customer_json['province'] = customer_table['province'].iloc[0]
        customer_json['city'] = customer_table['city'].iloc[0]
        for sku in customer_table['sku'].unique():
            customer_json[sku + str('_percent')] =  customer_table[customer_table['sku']==sku]['sku_percentage'].iloc[0]
        customer_json['sku_level']=json_df2[json_df2['customer_name']==i]['series_level'].iloc[0]
        doughnut_ls_comp.append(customer_json)
    doughnut_json_comp['output'] = doughnut_ls_comp
    doughnut_json_comp['graph_description'] = 'doughnut graph company level'

     # bar_chart top level--location        
    table = final_table_nonindo.groupby(['product_name', 'city'])['amount_million'].sum().reset_index()
    table = table.round({'amount_million':2})    
   
    bar_chart_loc = {}
    bar_chart_loc['graph_description'] = 'bar_chart top performing location'
    bar_chart_loc['labels'] = list(final_table_nonindo['city'].unique())
    sku_value_list=[]
    sku_list=[]
    for sku in ['Starter Deck', 'Booster A', 'Booster B']:
        sku_list.extend([s for s in table['product_name'].unique() if sku in s])
    for i in sku_list:#table['product_name'].unique():
        value_dic ={}
        value_dic['label'] = i
        value_list = []
        for k in bar_chart_loc['labels']:
            value = table.loc[(table['product_name']== i)& (table['city'] == k)]['amount_million'].values
            if len(value) > 0:
                value_list.append(round(value[0],3))
            else:
                value_list.append(0)
        value_dic['data'] = value_list
        value_dic['backgroundColor'] = color_dic[i.split(';')[0]][int(i.split('Series ')[1])]
    
        sku_value_list.append(value_dic)
    bar_chart_loc['datasets'] = sku_value_list
   
    # sort by company
    table2 = final_table_nonindo.groupby(['product_name', 'customer_name'])['amount_million'].sum().reset_index()
    table2 = table2.round({'amount_million':2}) 
    bar_chart_com={}
    bar_chart_com['labels'] = list(final_table_nonindo['customer_name'].unique())
    bar_chart_com['graph_description'] = 'bar_chart top performing company'
    sku_value_list=[]
   

    for i in sku_list:#table['product_name'].unique():
        value_dic ={}
        value_dic['label'] = i
        value_list = []
        for k in bar_chart_com['labels']:
            value = table2.loc[(table2['product_name']== i)& (table2['customer_name'] == k)]['amount_million'].values
            if len(value) > 0:
                value_list.append(round(value[0],3))
            else:
                value_list.append(0)
        value_dic['data'] = value_list
        value_dic['backgroundColor'] = color_dic[i.split(';')[0]][int(i.split('Series ')[1])]
        sku_value_list.append(value_dic)
    
    bar_chart_com['datasets'] = sku_value_list
    
    # top performing companies in Bandung 
    table3 = final_table_nonindo.groupby(['product_name', 'city', 'customer_name'])['amount_million'].sum().reset_index()
    table3 = table3.round({'amount_million':2})  
    bar_comps_city={}
    bar_comps_city['graph_description'] = 'all companies wihtin one city level'
    sku_list=[]
    for sku in ['Starter Deck', 'Booster A', 'Booster B']:
        sku_list.extend([s for s in table['product_name'].unique() if sku in s])

    sku_list.extend(set(table['product_name'].unique())-set(sku_list))
    for city in final_table_nonindo['city'].unique():
      
        bar_comps = {}
        bar_comps['labels'] = list(final_table_nonindo[final_table_nonindo['city'] == city]['customer_name'].unique())
        sku_value_list=[]
        for i in sku_list:
            value_dic ={}
            value_dic['label'] = i
            value_list = []
            for k in bar_comps['labels']:
                value = table2.loc[(table2['product_name']== i)& (table2['customer_name'] == k)]['amount_million'].values
                if len(value) > 0:
                    value_list.append(round(value[0],3))
                else:
                    value_list.append(0)
            value_dic['data'] = value_list
            value_dic['backgroundColor'] = color_dic[i.split(';')[0]][int(i.split('Series ')[1])]
            sku_value_list.append(value_dic)
        
        bar_comps['datasets'] = sku_value_list
        bar_comps_city[city] = bar_comps 
        
    # bar chart to view specific company
    # stack bar for each company     
    
    stack_dic = {'Booster A': 2, 'Booster B': 3, 'Starter Deck':1}
    for i in final_table_nonindo['sku'].unique():
        if i not in stack_dic.keys():
            stack_dic[i] = max(list(stack_dic.values()))+1


    table = final_table_nonindo.\
    groupby(['customer_name', 'year', 'month', 'product_name',\
             'week_of_month'])['amount_million'].sum().reset_index()#level=-1)
    table = table.round({'amount_million':2})           
    table[['year','month','week_of_month']]=table[['year','month','week_of_month']].applymap(lambda x: int(x))    
    
    
    table['beg_date']=table[['year','week_of_month']].apply(lambda x: getDateRangeFromWeek(x['year'],x['week_of_month'])[0],1)
    table['end_date']=table[['year','week_of_month']].apply(lambda x: getDateRangeFromWeek(x['year'],x['week_of_month'])[1],1)
    table['date_range']=table['beg_date'].astype(str)+' to '+ table['end_date'].astype(str)
    table['backgroundColor'] = table['product_name'].apply(lambda x: color_dic[x.split(';')[0]][int(x.split('Series ')[1])])
    table['stack'] = table['product_name'].apply(lambda x: stack_dic[x.split(';')[0]])
    table['index'] = [i for i in range(len(table))]
    table['date_label']=table[['date_range','week_of_month']].apply(lambda x: x['date_range']+' '+'week '+ str(x['week_of_month']),1)
    table=table.rename(columns={'product_name':'label'})
    final_chart_json={}
    final_chart=[]
    for dc in table['customer_name'].unique():
        dc_dic={}
       
        dc_table = table[table['customer_name']==dc].reset_index(drop=True)
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
    
        
     # bar chart to view specific company

    json_list = [topcomp_json,
                topltn_json,
                topprodct_json,
                geochart_prov_json,
                geochart_citi_json,
                geochart_comp_json,
                doughnut_json_comp,
                 bar_chart_loc,
                bar_chart_com,
                bar_comps_city,
                final_chart_json]    
                                  
    return json_list

