import os, sys
#Add parent dir and sub dirs to the python path for importing the modules from different directories
currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
rootdir = os.path.dirname(parentdir)
sys.path.extend([rootdir, parentdir])

# from lifetimes.utils import summary_data_from_transaction_data
# from lifetimes import BetaGeoFitter
from geo_fitter import BetaGeoFitter
# from lifetimes.utils import calibration_and_holdout_data
from lifetimes import GammaGammaFitter
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA

from bi360_insight.database.database_connectivity import SQLWrapper

import json
from datetime import datetime
import pandas as pd
import numpy as np





sql_wrapper = SQLWrapper()

def find_first_transactions(transactions, customer_id_col, datetime_col, monetary_value_col=None, datetime_format=None,
                            observation_period_end=datetime.today(), freq='D'):
    select_columns = [customer_id_col, datetime_col]

    if monetary_value_col:
        select_columns.append(monetary_value_col)

    transactions = transactions[select_columns].copy()

    # make sure the date column uses datetime objects, and use Pandas' DateTimeIndex.to_period()
    # to convert the column to a PeriodIndex which is useful for time-wise grouping and truncating
    transactions[datetime_col] = pd.to_datetime(transactions[datetime_col], format=datetime_format)
    transactions = transactions.set_index(datetime_col).to_period(freq)

    transactions = transactions.loc[(transactions.index <= observation_period_end)].reset_index()

    period_groupby = transactions.groupby([datetime_col, customer_id_col], sort=False, as_index=False)

    if monetary_value_col:
        # when we have a monetary column, make sure to sum together any values in the same period
        period_transactions = period_groupby.sum()
    else:
        # by calling head() on the groupby object, the datetime_col and customer_id_col columns
        # will be reduced
        period_transactions = period_groupby.head(1)

    # initialize a new column where we will indicate which are the first transactions
    period_transactions['first'] = False
    # find all of the initial transactions and store as an index
    first_transactions = period_transactions.groupby(customer_id_col, sort=True, as_index=False).head(1).index
    # mark the initial transactions as True
    period_transactions.loc[first_transactions, 'first'] = True
    select_columns.append('first')

    return period_transactions[select_columns]

def summary_data_from_transaction_data(transactions, customer_id_col, datetime_col, monetary_value_col=None, datetime_format=None,
                                       observation_period_end=datetime.today(), freq='D'):
    observation_period_end = pd.to_datetime(observation_period_end, format=datetime_format).to_period(freq)

    # label all of the repeated transactions
    repeated_transactions = find_first_transactions(
        transactions,
        customer_id_col,
        datetime_col,
        monetary_value_col,
        datetime_format,
        observation_period_end,
        freq
    )
    # count all orders by customer.
    customers = repeated_transactions.groupby(customer_id_col, sort=False)[datetime_col].agg(['min', 'max', 'count'])

    # subtract 1 from count, as we ignore their first order.
    customers['frequency'] = customers['count'] - 1

    customers['T'] = (observation_period_end - customers['min'])
    customers['recency'] = (customers['max'] - customers['min'])

    summary_columns = ['frequency', 'recency', 'T']

    if monetary_value_col:
        # create an index of all the first purchases
        first_purchases = repeated_transactions[repeated_transactions['first']].index
        # by setting the monetary_value cells of all the first purchases to NaN,
        # those values will be excluded from the mean value calculation
        repeated_transactions.loc[first_purchases, monetary_value_col] = np.nan

        customers['monetary_value'] = repeated_transactions.groupby(customer_id_col)[monetary_value_col].mean().fillna(0)
        summary_columns.append('monetary_value')
    customers['recency'] = customers['recency'].apply(lambda x: x.n)
    customers['T'] = customers['T'].apply(lambda x: x.n)
    return customers[summary_columns].astype(float)

def calibration_and_holdout_data(transactions, customer_id_col, datetime_col, calibration_period_end,
                                 observation_period_end=datetime.today(), freq='D', datetime_format=None,
                                 monetary_value_col=None):
    def to_period(d):
        return d.to_period(freq)

    transaction_cols = [customer_id_col, datetime_col]
    if monetary_value_col:
        transaction_cols.append(monetary_value_col)
    transactions = transactions[transaction_cols].copy()

    transactions[datetime_col] = pd.to_datetime(transactions[datetime_col], format=datetime_format)
    observation_period_end = pd.to_datetime(observation_period_end, format=datetime_format)
    calibration_period_end = pd.to_datetime(calibration_period_end, format=datetime_format)

    # create calibration dataset
    calibration_transactions = transactions.loc[transactions[datetime_col] <= calibration_period_end]
    calibration_summary_data = summary_data_from_transaction_data(calibration_transactions,
                                                                  customer_id_col,
                                                                  datetime_col,
                                                                  datetime_format=datetime_format,
                                                                  observation_period_end=calibration_period_end,
                                                                  freq=freq,
                                                                  monetary_value_col=monetary_value_col)
    calibration_summary_data.columns = [c + '_cal' for c in calibration_summary_data.columns]

    # create holdout dataset
    holdout_transactions = transactions.loc[(observation_period_end >= transactions[datetime_col]) &
                                           (transactions[datetime_col] > calibration_period_end)]
    holdout_transactions[datetime_col] = holdout_transactions[datetime_col].map(to_period)
    holdout_summary_data = holdout_transactions.groupby([customer_id_col, datetime_col], sort=False).agg(lambda r: 1)\
                                               .groupby(level=customer_id_col).agg(['count'])
    holdout_summary_data.columns = ['frequency_holdout']
    if monetary_value_col:
        holdout_summary_data['monetary_value_holdout'] = \
            holdout_transactions.groupby(customer_id_col)[monetary_value_col].mean()

    combined_data = calibration_summary_data.join(holdout_summary_data, how='left')
    combined_data.fillna(0, inplace=True)

    delta_time = to_period(observation_period_end) - to_period(calibration_period_end)
    combined_data['duration_holdout'] = delta_time

    return combined_data


def get_customers_segment_by_lifetime():
    customers_data = sql_wrapper.fetch_table_data('customers_dim')
    orders_data = sql_wrapper.fetch_table_data('orders_fact')
    order_status_data = sql_wrapper.fetch_table_data('order_status_dim')
    payments_data = sql_wrapper.fetch_table_data('payments_dim')
    customers_df = pd.DataFrame(
        customers_data, 
        columns = [
            'customer_id',
            'customer_unique_id',
            'customer_zip_code_prefix',
            'customer_city',
            'customer_state'
        ]
    )
    orders_df = pd.DataFrame(
        orders_data, 
        columns = [
            'order_id',
            'order_item_id',
            'product_id',
            'seller_id',
            'shipping_limit_date',
            'price',
            'freight_value'
        ]
    )
    order_status_df = pd.DataFrame(
        order_status_data, 
        columns = [
            'order_id',
            'customer_id',
            'order_status',
            'order_purchase_timestamp',
            'order_approved_at',
            'order_delivered_carrier_date',
            'order_delivered_customer_date',
            'order_estimated_delivery_date'
        ]
    )
    payments_df = pd.DataFrame(
        payments_data, 
        columns = [
            'order_id',
            'payment_sequential',
            'payment_type',
            'payment_installments',
            'payment_value'
        ]
    )
    orders_df = pd.merge(orders_df, order_status_df, on='order_id')
    orders_df = orders_df[['customer_id', 'order_id', 'order_purchase_timestamp']]
    customers_df = customers_df[['customer_id', 'customer_unique_id']]
    payments_df = payments_df[['order_id', 'payment_value']]
    segment_df = pd.merge(orders_df, customers_df, how='inner', on='customer_id')
    segment_df = pd.merge(segment_df, payments_df, how='inner', on='order_id')
    segment_df = segment_df.drop(['customer_id', 'order_id'], axis=1)
    segment_df.drop_duplicates(inplace=True)
    segment_df['order_purchase_timestamp'] = pd.to_datetime(segment_df['order_purchase_timestamp'], format="%Y-%m-%d %H:%M:%S")
    segment_df['order_purchase_timestamp'] = segment_df.order_purchase_timestamp.dt.date
    segment_df['order_purchase_timestamp'] = pd.to_datetime(segment_df['order_purchase_timestamp'])
    today = '2018-08-29'
    date_today = datetime.strptime(today, '%Y-%m-%d')
    r = segment_df.groupby('customer_unique_id').agg(['min', 'max'])['order_purchase_timestamp']
    r['recency'] = r['max'] - r['min']
    r['T'] = date_today - r['min']
    r = r[['recency', 'T']]
    aggregations = {
    'order_purchase_timestamp':'count',
    'payment_value': 'sum'}
    f = segment_df.groupby('customer_unique_id').agg(aggregations)
    f['frequency'] = f['order_purchase_timestamp'] - 1
    aggregations = f[['frequency']]
    ####
    rf = pd.merge(r,f, left_index=True, right_index=True)
    rfm = summary_data_from_transaction_data(segment_df, customer_id_col='customer_unique_id', datetime_col='order_purchase_timestamp', 
                                    monetary_value_col ='payment_value', observation_period_end='2018-08-29', 
                                    datetime_format='%Y-%m-%d', freq='W')
    bgf = BetaGeoFitter(penalizer_coef=0.001)
    bgf.fit(rfm['frequency'], rfm['recency'], rfm['T'], verbose=True)
    t = 4
    rfm['expected_4week'] = round(bgf.conditional_expected_number_of_purchases_up_to_time(t, rfm['frequency'], rfm['recency'], rfm['T']), 2)
    rfm.sort_values(by='expected_4week', ascending=False)
    rfm['expected_8week'] = round(bgf.predict(8, rfm['frequency'], rfm['recency'], rfm['T']), 2)
    rfm['expected_12week'] = round(bgf.predict(12, rfm['frequency'], rfm['recency'], rfm['T']), 2)
    t=12
    random_person = rfm.iloc[65432]
    prediction = bgf.predict(t, random_person['frequency'], random_person['recency'], random_person['T'])
    rfm.sort_values(by='expected_12week', ascending=False)
    rfm_val = calibration_and_holdout_data(segment_df, customer_id_col='customer_unique_id', datetime_col='order_purchase_timestamp', 
                                    monetary_value_col ='payment_value', calibration_period_end='2018-05-29',
                                    observation_period_end='2018-08-29', datetime_format='%Y-%m-%d', freq='W')
    bgf_val = BetaGeoFitter(penalizer_coef=0.001)
    bgf_val.fit(rfm_val['frequency_cal'], rfm_val['recency_cal'], rfm_val['T_cal'], verbose=True) ###can be plotted
    rfm_gg = rfm[rfm['frequency'] > 0]
    ggf = GammaGammaFitter(penalizer_coef = 0.0)
    ggf.fit(rfm_gg['frequency'], rfm_gg['monetary_value'])
    rfm['avg_transaction'] = round(ggf.conditional_expected_average_profit(rfm_gg['frequency'],
                                                   rfm_gg['monetary_value']), 2)
    rfm['avg_transaction'] = rfm['avg_transaction'].fillna(0)
    rfm.sort_values(by='avg_transaction', ascending=False)
    rfm['CLV'] = round(ggf.customer_lifetime_value(bgf, rfm['frequency'],
                    rfm['recency'], rfm['T'], rfm['monetary_value'],
                    time=26, discount_rate=0.01))
    rfm.sort_values(by='CLV', ascending=False)
    clusters = rfm.drop(rfm.iloc[:, 0:4], axis=1)
    scaler = StandardScaler()
    scaled = scaler.fit_transform(clusters)
    wcss = []
    for i in range(1, 6):
        kmeans = KMeans(n_clusters=i, max_iter=1000, random_state=0)
        kmeans.fit(scaled)
        wcss.append(kmeans.inertia_)
    model = KMeans(n_clusters = 3, max_iter = 1000)
    model.fit(scaled)
    labels = model.labels_
    clusters['cluster'] = labels
    features = ["expected_4week", "expected_8week", "expected_12week", "avg_transaction", "CLV"]
    pca = PCA(n_components=2)
    components = pca.fit_transform(clusters[features])
    npc = np.array(components)
    dfc = pd.DataFrame(npc, columns=['PC1','PC2'])
    dfc = dfc.set_index(clusters.index)
    dfc['label'] = clusters['cluster']
    clusters.groupby('cluster').agg(['max','min'])['CLV']
    clusters['cluster'].replace(to_replace=[0,1,2], value = ['Non-Profitable', 'Very Profitable', 'Profitable'], inplace=True)
    clusters.sort_values(by='CLV', ascending=False)
    clusters.to_csv('lifetime_segmentation.csv')


if __name__ == '__main__':
    get_customers_segment_by_lifetime()