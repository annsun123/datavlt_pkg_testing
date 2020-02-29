import pandas as pd
import numpy as np
from sklearn.metrics import silhouette_score
from sklearn.cluster import KMeans
from sklearn.cluster import AgglomerativeClustering
from sklearn.mixture import GaussianMixture
import warnings
from sklearn.exceptions import DataConversionWarning
from sklearn.preprocessing import MinMaxScaler
from codes.class_logging import logging_func
warnings.filterwarnings(action='ignore', category=DataConversionWarning)
rfm = logging_func('rfm_log', filepath='')
rfmlogger = rfm.myLogger()


def createRFM_dataset(df):
    df_rfm = df.groupby(['customer_name',
                         'customer_id',
                         'city']).agg({'invoice_date': 'max',
                                        'Avg_order_gap': 'mean',
                                        'qty_packs': 'count',
                                        'amount': 'sum'}).reset_index()
    df_rfm['Recency'] = [
        pd.to_datetime(
            df_rfm.loc[:,
                       'invoice_date']).max() - x for x in pd.to_datetime(df_rfm.loc[:, 'invoice_date']
                                                                         )
    ]
    df_rfm['Recency'] = df_rfm['Recency'].dt.days
    df_rfm.rename(columns={'amount': 'Monetary',
                           'qty_packs': 'Frequency'}, inplace=True)
    df_rfm.drop('invoice_date', axis=1, inplace=True)
    df_rfm['Avg_order_gap'] = [round(i) for i in df_rfm.Avg_order_gap.values.tolist()]

    value_recen=df_rfm['Recency'].drop_duplicates()
    df_rfm['R_score'] = pd.qcut(df_rfm['Recency'],
                                4,
                                labels=['4', '3', '2', '1']
                               ).astype(int) # duplicates='drop'
    value_fre=df_rfm['Frequency'].drop_duplicates()                           
    df_rfm['F_score'] = pd.cut(df_rfm['Frequency'],
                               4,
                               labels=['1', '2', '3', '4']
                               ).astype(int)#duplicates='drop'
    value_mon=df_rfm['Monetary'].drop_duplicates()                             
    df_rfm['M_score'] = pd.qcut(df_rfm['Monetary'],
                                4,
                                labels=['1', '2', '3', '4']
                               ).astype(int)
    df_rfm['RFM_score'] = df_rfm.apply(lambda x: (int(x.R_score) + int(x.F_score) + int(x.M_score)) / 3,
                                       axis=1)
    return df_rfm


# clustering
def clustering(df, str_cols, opt_clust):
    np.random.seed(999)
    scaler = MinMaxScaler()
    if opt_clust.lower() == 'kmeans':
        cluster = KMeans(init='k-means++', n_clusters=4,
                         n_init=100, random_state=50)
        pred_clusters = cluster.fit(scaler.fit_transform(df[str_cols])).labels_

    elif opt_clust.lower() == 'algo':
        cluster = AgglomerativeClustering(n_clusters=4,
                                          affinity='euclidean',
                                          linkage='ward')
        pred_clusters = cluster.fit(scaler.fit_transform(df[str_cols])).labels_
    elif opt_clust.lower() == 'gmm':
        cluster = GaussianMixture(n_components=4,
                                  random_state=42)
        pred_clusters = cluster.fit_predict(df[str_cols])
    else:
        rfmlogger.error('cluster option doesnt match')
    rfmlogger.info('silhouette score', silhouette_score(scaler.fit_transform(df[str_cols]),
                                                       pred_clusters,
                                                       random_state=42))
    return ['cluster-' + str(x + 1) for x in pred_clusters]


def defineCluster(df, str_cluter_def):
    df_agg = df.groupby('cluster').agg({'R_score': 'mean',
                                        'F_score': 'mean',
                                        'M_score': 'mean',
                                        'RFM_score': 'mean'})

    df_agg.sort_values('RFM_score', ascending=False, inplace=True)
    dict_clust_def_overall = dict(zip(df_agg.index, str_cluter_def))
    df_agg.index = dict_clust_def_overall.values()

    for i in range(4):
        df.loc[df['cluster'] == list(dict_clust_def_overall.keys())[i], 'cluster'] = list(dict_clust_def_overall.values())[i]

    return df
