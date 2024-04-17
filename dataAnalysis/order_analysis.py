import os, sys
#Add parent dir and sub dirs to the python path for importing the modules from different directories
currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
rootdir = os.path.dirname(parentdir)
sys.path.extend([rootdir, parentdir])

from bi360_insight.database.database_connectivity import SQLWrapper

import json
from datetime import datetime
import pandas as pd
import numpy as np



sql_wrapper = SQLWrapper()


def get_orders_summary():
    order_data = sql_wrapper.fetch_table_data('orders_fact')
    order_dim = sql_wrapper.fetch_table_data('order_status_dim')
    order_approved = [ord[4] for ord in order_dim if ord[4] is not None]
    order_approved = [datetime.strptime(ord, "%Y-%m-%d %H:%M:%S") for ord in order_approved]
    order_delivered = [ord[6] for ord in order_dim if ord[6] is not None]
    order_delivered = [datetime.strptime(ord, "%Y-%m-%d %H:%M:%S") for ord in order_delivered]
    delivery = zip(order_approved, order_delivered)
    delivery_days = [(avg_del[1] - avg_del[0]).days for avg_del in delivery]
    avg_delivery_days = round(sum(delivery_days)/len(delivery_days), 2)
    total_orders = len(order_data)
    frieght_value = [od[-1] for od in order_data]
    avg_frieght_value = sum(frieght_value)/len(frieght_value)
    price = [od[-2] for od in order_data]
    avg_price = sum(price)/len(price)
    result = {"Total Orders": total_orders, "Average Order Price":round(avg_price,2), 
              "Average Frieght Value":round(avg_frieght_value,2), "Average Delivery Days": avg_delivery_days}
    return result


def get_order_status():
    status_df = sql_wrapper.fetch_table_data('order_status_dim')
    status_df = pd.DataFrame(
        status_df, 
        columns=[
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
    delivery_status = status_df['order_status'].unique()
    order_status = {}
    for ds in delivery_status:
        order_status[ds] = round((len(status_df[status_df['order_status'] == ds])/len(status_df)) * 100, 2)
    return order_status

def get_order_time():
    order_status_data = sql_wrapper.fetch_table_data('order_status_dim')
    status_df = pd.DataFrame(
        order_status_data, 
        columns=[
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
    orders_fmt = status_df.copy()
    orders_fmt["order_purchase_timestamp"] = pd.to_datetime(status_df["order_purchase_timestamp"], format='%Y-%m-%d %H:%M:%S')
    orders_fmt["order_delivered_carrier_date"] = pd.to_datetime(status_df["order_delivered_carrier_date"], format='%Y-%m-%d %H:%M:%S')
    orders_fmt["order_delivered_customer_date"] = pd.to_datetime(status_df["order_delivered_customer_date"], format='%Y-%m-%d %H:%M:%S')
    orders_fmt["order_estimated_delivery_date"] = pd.to_datetime(status_df["order_estimated_delivery_date"], format='%Y-%m-%d %H:%M:%S')
    orders_fmt['order_purchase_hour'] = orders_fmt['order_purchase_timestamp'].apply(lambda x: x.hour)
    hours_bins = [-0.1, 6, 12, 18, 23]
    hours_labels = ['Sunrise', 'Morning', 'Afternoon', 'Night']
    orders_fmt['order_purchase_time_day'] = pd.cut(orders_fmt['order_purchase_hour'], hours_bins, labels=hours_labels)
    order_time = orders_fmt['order_purchase_time_day'].unique()
    order_purchase_time = {}
    for tm in order_time:
        order_purchase_time[tm] = len(orders_fmt[orders_fmt['order_purchase_time_day'] == tm])
    return order_purchase_time

def get_late_deliveries():
    order_status_data = sql_wrapper.fetch_table_data('order_status_dim')
    status_df = pd.DataFrame(
        order_status_data, 
        columns=[
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
    orders_fmt = status_df.copy()
    orders_fmt["order_purchase_timestamp"] = pd.to_datetime(status_df["order_purchase_timestamp"], format='%Y-%m-%d %H:%M:%S')
    orders_fmt["order_delivered_carrier_date"] = pd.to_datetime(status_df["order_delivered_carrier_date"], format='%Y-%m-%d %H:%M:%S')
    orders_fmt["order_delivered_customer_date"] = pd.to_datetime(status_df["order_delivered_customer_date"], format='%Y-%m-%d %H:%M:%S')
    orders_fmt["order_estimated_delivery_date"] = pd.to_datetime(status_df["order_estimated_delivery_date"], format='%Y-%m-%d %H:%M:%S')
    orders_fmt['order_purchase_hour'] = orders_fmt['order_purchase_timestamp'].apply(lambda x: x.hour)
    orders_fmt['day_to_delivery']=(orders_fmt['order_delivered_customer_date']-orders_fmt['order_purchase_timestamp']).dt.days
    # comparing the estimated delivery time and actual delivery time
    orders_fmt['delivery'] = (orders_fmt['order_estimated_delivery_date']-orders_fmt['order_delivered_customer_date']).dt.days
    orders_fmt['late_delivery'] = np.where(orders_fmt['delivery'] >= 0, False, True)
    late_delivery = orders_fmt['late_delivery'].unique()
    late_delivery_status = {}
    for ld in late_delivery:
        late_delivery_status[str(ld)] = len(orders_fmt[orders_fmt['late_delivery'] == ld])
    return late_delivery_status

def get_payment_types():
    order_status_data = sql_wrapper.fetch_table_data('order_status_dim')
    pass

def get_num_orders_per_year():
    order_status_data = sql_wrapper.fetch_table_data('order_status_dim')
    status_df = pd.DataFrame(
        order_status_data, 
        columns=[
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
    orders_fmt = status_df.copy()
    orders_fmt["order_purchase_timestamp"] = pd.to_datetime(status_df["order_purchase_timestamp"], format='%Y-%m-%d %H:%M:%S')
    orders_fmt['Year'] = orders_fmt['order_purchase_timestamp'].dt.year
    total_years = orders_fmt['Year'].unique()
    orders_per_year = {}
    for yr in total_years:
        orders_per_year[str(yr)] = len(orders_fmt[orders_fmt['Year'] == yr])
    return orders_per_year
    # import pdb;pdb.set_trace()

def get_top_states_by_order():
    customers_data = sql_wrapper.fetch_table_data('customers_dim')
    order_status_data = sql_wrapper.fetch_table_data('order_status_dim')
    sellers_data = sql_wrapper.fetch_table_data('sellers_dim')
    orders_data = sql_wrapper.fetch_table_data('orders_fact')
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
    sellers_df = pd.DataFrame(
        sellers_data, 
        columns=[
            'seller_id', 
            'seller_zip_code_prefix', 
            'seller_city', 
            'seller_state'
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
    
    order_df = order_status_df.merge(orders_df, on='order_id').merge(customers_df, on='customer_id', how='left').merge(sellers_df, on='seller_id', how='left').merge(payments_df, on='order_id', how='left')
    top10_state = order_df.groupby('customer_state')\
                                .agg(num_orders = ('order_id','nunique'),
                                    revenue = ('payment_value', 'sum'))\
                                .sort_values('revenue', ascending=False)[:10]
    top10_state = top10_state[['num_orders']]
    top10_state = top10_state.iloc[:,0]
    return top10_state.to_dict()
  



# if __name__ == "__main__":
    # get_order_time()
    # get_orders_summary()
    # get_order_status()
    # get_late_deliveries()
    # get_num_orders_per_year()
    # get_top_states_by_order()