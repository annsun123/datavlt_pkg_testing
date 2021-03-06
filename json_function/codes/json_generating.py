from json_function.codes.supporting_function import creating_graph_json, color_variant, getDateRangeFromWeek
from json_function.codes.logging import logging_func
import pandas as pd
import numpy as np
import random
import datetime

json_function=logging_func('jsongeneration_log',filepath='/')
jgntlogger = json_function.myLogger()

def creating_json_nonindo(final_table_nonindo): 
   # final_table_nonindo['qty_mc'] = final_table_nonindo[['sku','qty_mc']].apply(lambda x: x['qty_mc']/120 if x['sku']=='STARTER DECK'  else x['qty_mc']/720,1)
    
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
    topltn_final_table_nonindo = final_table_nonindo[['province', 'city','qty_mc','amount_million', \
                                  'customer_id']].groupby(['province','city']).\
                                  agg({'qty_mc':'sum', 'amount_million':'sum'}).\
                                  reset_index()
    topltn_final_table_nonindo = topltn_final_table_nonindo.round({'amount_million':2, 'qty_mc':2})
  #  topltn_final_table_nonindo['qty_mc'] = topltn_final_table_nonindo['qty_mc'].apply(lambda x: round(x)) 
    topltn_final_table_nonindo = topltn_final_table_nonindo.astype(object).where(topltn_final_table_nonindo.notnull(), None)                           
    topltn_json = creating_graph_json(topltn_final_table_nonindo, \
                                      graph_description = 'top_performing_location', \
                                      filter_link = 'NA')

    # top performing companies 
    topcomp_final_table_nonindo = final_table_nonindo[['customer_name', 'qty_mc','amount_million', \
                                  'customer_id']].groupby('customer_name').\
                                  agg({'qty_mc':'sum', 'amount_million':'sum'}).\
                                  reset_index()
    topcomp_final_table_nonindo = topcomp_final_table_nonindo.round({'amount_million':2, 'qty_mc':2})                              
    #topcomp_final_table_nonindo['qty_mc'] = topcomp_final_table_nonindo['qty_mc'].apply(lambda x: round(x)) 
    topcomp_final_table_nonindo = topcomp_final_table_nonindo.astype(object).where(topcomp_final_table_nonindo.notnull(), None)                              
    topcomp_json = creating_graph_json(topcomp_final_table_nonindo, \
                                     graph_description = 'top_performing_companies', \
                                     filter_link = 'NA')
                  
                                
    # top performing products  
    
    topprodct_final_table_nonindo = final_table_nonindo[['product_name', 'qty_mc', 'amount_million', \
                                  'customer_id']].groupby('product_name').\
                                  agg({'qty_mc': 'sum', 'amount_million': 'sum'}). \
                                  reset_index()
    topprodct_final_table_nonindo = topprodct_final_table_nonindo.round({'amount_million':2, 'qty_mc':2}) 
    #topprodct_final_table_nonindo['qty_mc'] = topprodct_final_table_nonindo['qty_mc'].apply(lambda x: round(x))                               
    topprodct_final_table_nonindo = topprodct_final_table_nonindo.astype(object).where(topprodct_final_table_nonindo.notnull(), None)                                 
    topprodct_json = creating_graph_json(topprodct_final_table_nonindo,\
                                         graph_description = 'top_performing_product', \
                                         filter_link = 'NA')
                      
    # creating json for all different graphs 
    
    geochart_prov_final_table_nonindo = final_table_nonindo[['province', 'qty_mc', 'amount_million']].groupby('province').\
                                  agg({'qty_mc': 'sum', 'amount_million': 'sum'}). \
                                  reset_index()
    geochart_prov_final_table_nonindo = geochart_prov_final_table_nonindo.round({'amount_million':2, 'qty_mc':2})
    
   # geochart_prov_final_table_nonindo['qty_mc'] = geochart_prov_final_table_nonindo['qty_mc'].apply(lambda x: round(x))
    geochart_prov_final_table_nonindo = geochart_prov_final_table_nonindo.sort_values(by='province')
    np.random.seed(123)
    for index,prov in enumerate(geochart_prov_final_table_nonindo['province'].unique()):  
        geochart_prov_final_table_nonindo.loc[geochart_prov_final_table_nonindo['province'] == prov, 'province_color'] = color_list[index]
                  
    geochart_prov_json = {}                                 
    contry_revenue = geochart_prov_final_table_nonindo['amount_million'].sum()
    geochart_prov_final_table_nonindo['percentage'] = (geochart_prov_final_table_nonindo['amount_million'] / contry_revenue)*100
    geochart_prov_final_table_nonindo['percentage'] = [round(i,2) for i in geochart_prov_final_table_nonindo.percentage.values.tolist()]
    geochart_prov_json['total_revenue'] = round(contry_revenue, 2)
    geochart_prov_final_table_nonindo = geochart_prov_final_table_nonindo.astype(object).where(geochart_prov_final_table_nonindo.notnull(),None)
    value = geochart_prov_final_table_nonindo.to_dict(orient='records')
    geochart_prov_json['output'] = value    
    geochart_prov_json['graph_description'] = 'all provinces within city'
                                                 
    # geo-chart on level of all companies in one province
    geochart_citi_final_table_nonindo = final_table_nonindo[['province', 'city', 'qty_mc', 'amount_million']].groupby(['province', 'city']).\
                                  agg({'qty_mc': 'sum', 'amount_million': 'sum'}). \
                                  reset_index(level=1)
    geochart_citi_final_table_nonindo = geochart_citi_final_table_nonindo.round({'amount_million':2, 'qty_mc':2})
    #geochart_citi_final_table_nonindo['qty_mc'] = geochart_citi_final_table_nonindo['qty_mc'].apply(lambda x: round(x))                             
    #total revenue for each province
                 
    table = final_table_nonindo.groupby('province')['amount_million'].sum().round(2).to_frame().rename(columns={'amount_million': 'province_total_amount_million'})
    
    geochart_citi_final_table_nonindo['city_color']=0           
   
    for i in table.index:  
        geochart_citi_final_table_nonindo.loc[i,  'city_percentage']= (geochart_citi_final_table_nonindo.loc[i, 'amount_million']/table.loc[i,'province_total_amount_million'])*100
        length = [1 if type(geochart_citi_final_table_nonindo.loc[i]['city'])==str else len(geochart_citi_final_table_nonindo.loc[i])][0]
        if length==1:
            geochart_citi_final_table_nonindo.loc[i, 'city_color']=random.sample(color_list,length)[0]
        else:
            geochart_citi_final_table_nonindo.loc[i, 'city_color']=random.sample(color_list,length)
    geochart_citi_final_table_nonindo['city_percentage']=[round(i,2) for i in geochart_citi_final_table_nonindo.city_percentage.values.tolist()]
    
    geochart_citi_final_table_nonindo['longtitude'], geochart_citi_final_table_nonindo['latitude']=[np.nan, np.nan]
    
    for i in geochart_citi_final_table_nonindo['city'].unique():
        table_x = final_table_nonindo[final_table_nonindo['city']==i]
        try:
            geochart_citi_final_table_nonindo.loc[geochart_citi_final_table_nonindo['city'] == i, ['longtitude', 'latitude']] = list(table_x[~table_x['latitude'].isnull()][['longtitude', 'latitude']].values[0])
        except:
            geochart_citi_final_table_nonindo.loc[geochart_citi_final_table_nonindo['city'] == i, ['longtitude', 'latitude']] = [np.nan, np.nan]
            
    geochart_citi_final_table_nonindo = geochart_citi_final_table_nonindo.reset_index().merge(table.reset_index(), how='left', on='province')
    geochart_citi_final_table_nonindo['size_category']=pd.qcut(geochart_citi_final_table_nonindo['amount_million'],4, labels=[i for i in range(4)])
  
    geochart_citi_final_table_nonindo = geochart_citi_final_table_nonindo.astype(object).where(geochart_citi_final_table_nonindo.notnull(), None) 
    geochart_citi_json = creating_graph_json(geochart_citi_final_table_nonindo,\
                                           graph_description = 'map chart all citie at each province level', \
                                           filter_link = 'None')   

    
    #  geo graph specific company level--total
    #getting cities geo coordinates 
    table=geochart_citi_final_table_nonindo.groupby(['province', 'city'])['amount_million'].sum().to_frame().rename(columns={'amount_million': 'city_total_amount_million'}).round(2)
    
    geochart_comp_final_table_nonindo = final_table_nonindo[['province', 'city', 'customer_name', 'qty_mc', 'amount_million']].groupby(['province', 'city', 'customer_name']).\
                                  agg({'qty_mc': 'sum', 'amount_million': 'sum'}). \
                                  reset_index(level=2)
    geochart_comp_final_table_nonindo = geochart_comp_final_table_nonindo.round({'amount_million':2, 'qty_mc':2})      
   # geochart_comp_final_table_nonindo['qty_mc'] = geochart_comp_final_table_nonindo['qty_mc'].apply(lambda x: round(x))                          
    geochart_comp_final_table_nonindo['color_category']=pd.qcut(geochart_comp_final_table_nonindo['amount_million'],4, labels=[3,2,1,0])   
    geochart_comp_final_table_nonindo['size_category']=pd.qcut(geochart_comp_final_table_nonindo['amount_million'],4, labels=[0,1,2,3])  
    geochart_comp_final_table_nonindo['company_percentage']=0
    for i in table.index:
        percentage=[round(value,2) for value in \
        (geochart_comp_final_table_nonindo.loc[i, 'amount_million']/table.loc[i,'city_total_amount_million']*100).tolist()]   
        
        geochart_comp_final_table_nonindo.loc[i,  'company_percentage']= percentage
    
    geochart_comp_final_table_nonindo['city_color']=0
    for index,value in enumerate(geochart_comp_final_table_nonindo.index):
        
        
        color =geochart_citi_final_table_nonindo.loc[(geochart_citi_final_table_nonindo['city']==geochart_comp_final_table_nonindo.index[index][1])&\
        (geochart_citi_final_table_nonindo['province']==geochart_comp_final_table_nonindo.index[index][0]),'city_color'].values[0]
        geochart_comp_final_table_nonindo.loc[value,'city_color'] = [color for time in range(len(geochart_comp_final_table_nonindo.loc[value,'city_color']))]
  
    
    geochart_comp_final_table_nonindo['company_color']=geochart_comp_final_table_nonindo[['city_color','color_category']].apply(lambda x: color_variant(x['city_color'], brightness_offset=20*x['color_category']) if x['color_category'] !=0 else x['city_color'],1)    
    company_geo_dic={}
    for i in final_table_nonindo['customer_name'].unique():
        company_geo_dic[i] = list(final_table_nonindo[final_table_nonindo['customer_name'] == i].iloc[0][['longtitude', 'latitude']].values)
    geochart_comp_final_table_nonindo['longtitude'] = geochart_comp_final_table_nonindo['customer_name'].apply(lambda x: company_geo_dic[x][0])
    geochart_comp_final_table_nonindo['latitude'] = geochart_comp_final_table_nonindo['customer_name'].apply(lambda x: company_geo_dic[x][1])    
    geochart_comp_final_table_nonindo=geochart_comp_final_table_nonindo.drop(['color_category'],1)
    geochart_comp_final_table_nonindo=geochart_comp_final_table_nonindo.reset_index()
    geochart_comp_final_table_nonindo = geochart_comp_final_table_nonindo.astype(object).where(geochart_comp_final_table_nonindo.notnull(), None) 
    geochart_comp_json = creating_graph_json(geochart_comp_final_table_nonindo,\
                                           graph_description = 'map chart all companies at each city level', \
                                           filter_link = 'None')   

    
      
    ### doughnut charts 
    table = final_table_nonindo[['province',\
                         'city', 'customer_name', 'qty_mc', \
                         'amount_million', 'sku', 'series_num']].\
                         groupby(['province','city', 'customer_name','sku', 'series_num']).\
                          agg({'qty_mc': 'sum', 'amount_million': 'sum'}).reset_index()
    table = table.round({'amount_million':2, 'qty_mc':2})
   # table['qty_mc'] = table['qty_mc'].apply(lambda x: round(x))    
    table['series_num'] = table['series_num'].apply(lambda x: 'series ' +str(x))
    table['backgroundColor'] = table['sku'].apply(lambda x: color_legend[x])
    table = table.merge(table.groupby(['province','city','customer_name'])['amount_million'].sum().round(2).reset_index().rename(columns={'amount_million':'comp_total_rev'}), how='left', on=['province','city','customer_name'])
    table = table.merge(table.groupby(['province','city','customer_name','sku'])['amount_million'].sum().round(2).reset_index().rename(columns={'amount_million':'sku_total_amount_million'}), how='left', on=['province','city','customer_name', 'sku'])
    
    table['sku_percentage'] = [round(value,2) for value in (table['sku_total_amount_million'] / table['comp_total_rev']*100).tolist()]
    table['series_percentage'] = [round(value,2) for value in (table['amount_million'] / table['comp_total_rev']*100).tolist()]
    table = table.rename(columns={'amount_million':'series_total_amount_million'})
    table = table[['province', 'city', 'customer_name', 'sku', 'series_num', 'qty_mc',\
                    'backgroundColor', 'comp_total_rev', 'sku_total_amount_million', 'sku_percentage',\
                   'series_total_amount_million','series_percentage' ]]

    table = table.astype(object).where(table.notnull(),None)  
    
    json_final_table_nonindo2 = table.groupby(['province','city','customer_name','sku','backgroundColor',\
                              'comp_total_rev','sku_total_amount_million',\
                              'sku_percentage'])[['series_num','qty_mc', 'series_total_amount_million','series_percentage']].\
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
        customer_json['sku_level']=json_final_table_nonindo2[json_final_table_nonindo2['customer_name']==i]['series_level'].iloc[0]
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
    table[['year','month','week_of_month']] = table[['year','month','week_of_month']].applymap(lambda x: int(x)) 
    table['date_label'] = table[['year','month']].apply(lambda x: datetime.date(x['year'],x['month'],1),1)
    
    
    min_date = final_table_nonindo['invoice_date'].min()
    max_date = final_table_nonindo['invoice_date'].max()
    
    df = pd.DataFrame()
    df_date = pd.DataFrame()
    idx=pd.date_range(min_date.strftime('%Y-%m-%d'), max_date.strftime('%Y-%m-%d'), freq='W')
    df_date['date']=idx
    df_date['year'] = df_date['date'].apply(lambda x: x.year)
    df_date['week_of_month'] = df_date['date'].apply(lambda x: x.week)
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

