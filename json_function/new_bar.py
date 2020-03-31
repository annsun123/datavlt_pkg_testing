# -*- coding: utf-8 -*-
"""
Created on Fri Feb 21 08:01:22 2020

@author: anyan.sun
"""

# -*- coding: utf-8 -*-
"""
Created on Fri Feb 21 07:50:17 2020

@author: anyan.sun
"""
'''
bar 
verifying dataset

bar solution:
	using the date range instead of week: just change the week number label to date range label. if there is no sales made, just leave it there. 
	using the 


convert to int table:
table[['year','month','week_of_month']] = table[['year','month','week_of_month']].applymap(lambda x: int(x))

begining date:
table['beg_date']=table.apply(lambda x: datetime.date(x['year'], x['month'], 1).isoformat() if x['week_of_month']==1 else datetime.date(x['year'], x['month'], (x['week_of_month']-2)*7+1+7-calendar.monthrange(x['year'],x['month'])[0]).isoformat(),1)

ending date:
table['end_date']=table.apply(lambda x: last_date(x['year'], x['month'], x['week_of_month']),1)

date_dic=table[['year','month','week_of_month']].drop_duplicates()

date_dic['beg_date']=date_dic.apply(lambda x: datetime.date(x['year'], x['month'], 1).isoformat() if x['week_of_month']==1 \
     else datetime.date(x['year'], x['month'],\
                        (x['week_of_month']-2)*7+1+7-calendar.monthrange(x['year'],\
                        x['month'])[0]).isoformat(),1)

date_dic['end_date']=date_dic.apply(lambda x: last_date(x['year'], x['month'], x['week_of_month']),1)

date_dic['date_label']=date_dic['beg_date']+' to '+date_dic['end_date']

date_dic_json=date_dic.groupby(['year', 'month', 'week_of_month'])[('beg_date','end_date')].apply(lambda x: x.to_dict(orient='records')).reset_index(name="data").to_dict(orient='records')
'''

#table_check.insert(1,'label',[['week 1', 'week 2', 'week 3', 'week 4'] for x in range(len(table_check))] )
conn=create_conn()
cur = conn.cursor()
final_table_indo = pd.read_sql_query("SELECT * from final_indo", conn)
cur.close()
conn.close()
import timeit
start = timeit.default_timer()

final_table_indo['city'] = final_table_indo['city'].apply(lambda x: x.split(',')[0])
final_table_indo[['city','province', 'sku']] = final_table_indo[['city','province', 'sku']].applymap(lambda x : x.title())
final_table_indo = final_table_indo.astype(object).where(final_table_indo.notnull(), None)
final_table_indo['amount'] = [round(float(x),2) for x in final_table_indo['amount'].apply(lambda x: '{:.2f}'.format(x))]

final_table_indo = final_table_indo.applymap(lambda x: x.strip() if type(x)==str else x)


colors_option = ['#00BD33', '#c5704b', '#BD009C', '#8B4513', '#8B8113']
          # [green, red, black, orange, brown, Olive]
color_legend = {'Booster A': '#101214', 'Booster B': '#fa0729', 'Starter Deck':'#0068BD'}

color_dic = {}
for sku in final_table_indo['sku'].unique():
    color_var = {}
    index = 0
    if sku in color_legend.keys():
        base_color = color_legend[sku]
    else:
        color_legend[sku] =  colors_option[index]
        base_color = colors_option[index]
    series = sorted([int(i) for i in final_table_indo[final_table_indo['sku'] == sku]['series_num'].unique()],reverse=False)
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
        
###########################
 
def last_date(year, month, week):
    try:
        last_date = datetime.date(year, month, (week-1)*7+7-calendar.monthrange(year,month)[0]).isoformat()
    except:
        last_date = datetime.date(year, month,calendar.monthrange(year, month)[1]).isoformat()
        
    return last_date

date_dic=table[['year','month','week_of_month']].drop_duplicates()

date_dic['beg_date']=date_dic.apply(lambda x: datetime.date(x['year'], x['month'], 1).isoformat() if x['week_of_month']==1 \
     else datetime.date(x['year'], x['month'],\
                        (x['week_of_month']-2)*7+1+7-calendar.monthrange(x['year'],\
                        x['month'])[0]).isoformat(),1)
date_dic['end_date']=date_dic.apply(lambda x: last_date(x['year'], x['month'], x['week_of_month']),1)
    

###########################        
final_table_indo['product_name'] = final_table_indo[['sku', 'series_num']].apply(lambda x: x['sku']+'; Series '+ str(x['series_num']), 1)
table = final_table_indo.\
groupby(['distribution_center', 'year', 'month', 'product_name',\
         'week_of_month'])['amount'].sum().reset_index()#level=-1)
table[['year','month','week_of_month']]=table[['year','month','week_of_month']].applymap(lambda x: int(x))    

table['index'] = [i for i in range(len(table))]
table['week_of_month_cat'] = table['week_of_month'].astype('category').cat.codes
table['index'] = table['index'].astype('category').cat.codes
table['amount']=round(table['amount'],2)
from scipy.sparse import csr_matrix
spar_matrix=csr_matrix((table['amount'], (table['index'], table['week_of_month_cat'])))
table2 = pd.DataFrame(spar_matrix.todense(), \
                      columns=[i for i in range(int(table['week_of_month'].min()),\
                                                int(table['week_of_month'].max())+1,1)])


table['value_list'] = 0


table3=table.join(pd.DataFrame({'data':list(table2.values)}))
table4 = table3.groupby(['distribution_center', 'year', 'month', 'beg_date', 'end_date', 'product_name'])['data'].apply(lambda x: list(np.sum(x)))
table4=table4.reset_index()
stack_dic = {'Booster A': 1, 'Booster B': 2, 'Starter Deck':3}
for i in final_table_indo['sku'].unique():
    if i not in stack_dic.keys():
        stack_dic[i] = max(list(stack_dic.values()))+1

table4['backgroundColor'] = table4['product_name'].apply(lambda x: color_dic[x.split(';')[0]][int(x.split('Series ')[1])])
table4['stack'] = table4['product_name'].apply(lambda x: stack_dic[x.split(';')[0]])
table4[['month', 'year']]=table4[['month', 'year']].applymap(lambda x: int(x))
bar_comp_json={}
comp_lst=[]
import calendar
month_dic={num: name for num, name in enumerate(calendar.month_abbr) if num}
for customer in table4['distribution_center'].unique():
    customer_json= {}
    customer_df = table4[table4['distribution_center'] == customer]
    customer_json['CompanyName']=customer



    month_lst=[]
    for month in customer_df['month'].unique():
        month_json={}
        month_json['month']=month_dic[month]

        table_check= customer_df[customer_df['month']==month]
        value_list=[]
        for i in table_check['data'].apply(lambda x: [(i) for i in range(len(x)) if x[i]!=0]):
            value_list.extend(i)
        value_list=list(set(value_list))
        #month_json['label']=['week '+str(i+1) for i in value_list]
        table_check['data']=table_check['data'].apply(lambda x: np.array(x)[value_list].tolist())
        
        value=table_check[['data','backgroundColor', 'product_name', 'stack']].to_dict(orient='records')
        month_json['dataForGraph']={'label':['week '+str(i+1) for i in value_list],'datasets':value}
        month_lst.append(month_json)
    customer_json['details']=month_lst
    comp_lst.append(customer_json)
bar_comp_json['graph_description'] = 'bar chart showing each company weekly sales'
bar_comp_json['data']=comp_lst

stop = timeit.default_timer()

print('Time: ', stop - start)  