import os, sys
#Add parent dir and sub dirs to the python path for importing the modules from different directories
currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
rootdir = os.path.dirname(parentdir)
sys.path.extend([rootdir, parentdir])

from bi360_insight.database.database_connectivity import SQLWrapper

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


def get_order_status(status_df):
    delivery_status = status_df['order_status'].unique()
    order_status = {}
    for ds in delivery_status:
        order_status[ds] = round((len(status_df[status_df['order_status'] == ds])/len(status_df)) * 100, 2)
    return order_status

def get_order_time(status_df):
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

def get_late_deliveries(status_df):
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

def get_num_orders_per_year(status_df):
    orders_fmt = status_df.copy()
    orders_fmt["order_purchase_timestamp"] = pd.to_datetime(status_df["order_purchase_timestamp"], format='%Y-%m-%d %H:%M:%S')
    orders_fmt['Year'] = orders_fmt['order_purchase_timestamp'].dt.year
    total_years = orders_fmt['Year'].unique()
    orders_per_year = {}
    for yr in total_years:
        orders_per_year[str(yr)] = len(orders_fmt[orders_fmt['Year'] == yr])
    return orders_per_year

def get_top_states_by_order(order_df):
    top10_state = order_df.groupby('customer_state')\
                                .agg(num_orders = ('order_id','nunique'),
                                    revenue = ('payment_value', 'sum'))\
                                .sort_values('revenue', ascending=False)[:10]
    top10_state = top10_state[['num_orders']]
    top10_state = top10_state.iloc[:,0]
    return top10_state.to_dict()
  