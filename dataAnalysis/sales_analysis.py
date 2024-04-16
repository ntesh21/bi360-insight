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

def get_sales_summary():
    # pass
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
    sales_fmt = status_df.copy()
    sales_fmt["order_purchase_timestamp"] = pd.to_datetime(status_df["order_purchase_timestamp"], format='%Y-%m-%d %H:%M:%S')
    sales_fmt["order_delivered_carrier_date"] = pd.to_datetime(status_df["order_delivered_carrier_date"], format='%Y-%m-%d %H:%M:%S')
    sales_fmt["order_delivered_customer_date"] = pd.to_datetime(status_df["order_delivered_customer_date"], format='%Y-%m-%d %H:%M:%S')
    sales_fmt["order_estimated_delivery_date"] = pd.to_datetime(status_df["order_estimated_delivery_date"], format='%Y-%m-%d %H:%M:%S')
    sales_fmt['month'] = sales_fmt['order_purchase_timestamp'].dt.strftime('%b%Y')
    sales_figures = sales_fmt.groupby('month').size().sort_values().to_dict()
    sales_summary = {}
    sales_summary["Average sales per Month"] = sum(list(sales_figures.values()))/len(list(sales_figures.values()))
    sales_summary["Month with highest Sales"] = max(sales_figures, key=sales_figures.get)
    sales_summary["Month with lowest Sales"] = min(sales_figures, key=sales_figures.get)
    return sales_summary
    # import pdb;pdb.set_trace()


def get_city_wise_sellers():
    seller_data = sql_wrapper.fetch_table_data('sellers_dim')
    seller_df = pd.DataFrame(
        seller_data, 
        columns=[
            'seller_id', 
            'seller_zip_code_prefix', 
            'seller_city', 
            'seller_state'
        ]
    )
    seller_city = seller_df["seller_city"].value_counts().sort_values(ascending=False)[:10]
    city_wise_seller = seller_city.to_dict()
    return city_wise_seller
    

def get_product_sold_per_category():
    customers_data = sql_wrapper.fetch_table_data('customers_dim')
    order_status_data = sql_wrapper.fetch_table_data('order_status_dim')
    orders_data = sql_wrapper.fetch_table_data('orders_fact')
    product_data = sql_wrapper.fetch_table_data('products_dim')
    tranlation_data = sql_wrapper.fetch_table_data('product_translation_dim')
    # payments_data = sql_wrapper.fetch_table_data('payments_dim')
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
    product_df = pd.DataFrame(
        product_data, 
        columns = [
            'product_id',
            'product_category_name',
            'product_name_length',
            'product_description_length',
            'product_photos_qty',
            'product_weight_g',
            'product_length_cm',
            'product_height_cm',
            'product_width_cm',
        ]
    )
    tranlation_df = pd.DataFrame(
        tranlation_data, 
        columns = [
            'product_category_name',
            'product_category_name_english',
        ]
    )
    products_df = pd.merge(order_status_df, customers_df, on='customer_id')
    products_df = products_df.merge(orders_df, on='order_id')
    products_df = products_df.merge(product_df, on='product_id')
    products_df = products_df.merge(tranlation_df, on='product_category_name')
    top_categories = products_df[['product_category_name_english', 'order_item_id']]
    top_categories = top_categories.groupby(['product_category_name_english']).sum().sort_values(by=['order_item_id'], ascending=False).reset_index()
    top_category = top_categories[:10]
    bottom_category = top_categories[-10:]
    return dict(zip(top_category.product_category_name_english, top_category.order_item_id)), dict(zip(bottom_category.product_category_name_english, bottom_category.order_item_id))

    

def get_payment_types():
    payments_data = sql_wrapper.fetch_table_data('payments_dim')
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
    return payments_df.groupby(['payment_type']).size().to_dict()






# if __name__ == '__main__':
#     get_city_wise_sellers() 
    # get_product_sold_per_category()
    # get_payment_types()
    # get_sales_summary()
    