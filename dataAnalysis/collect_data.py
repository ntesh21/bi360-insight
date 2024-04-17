import os, sys
#Add parent dir and sub dirs to the python path for importing the modules from different directories
currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
rootdir = os.path.dirname(parentdir)
sys.path.extend([rootdir, parentdir])

from bi360_insight.database.database_connectivity import SQLWrapper

import pandas as pd


sql_wrapper = SQLWrapper()


def get_master_data():
    customers_data = sql_wrapper.fetch_table_data('customers_dim')
    orders_data = sql_wrapper.fetch_table_data('orders_fact')
    order_status_data = sql_wrapper.fetch_table_data('order_status_dim')
    seller_data = sql_wrapper.fetch_table_data('sellers_dim')
    payments_data = sql_wrapper.fetch_table_data('payments_dim')
    product_data = sql_wrapper.fetch_table_data('products_dim')
    tranlation_data = sql_wrapper.fetch_table_data('product_translation_dim')

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
    seller_df = pd.DataFrame(
        seller_data, 
        columns=[
            'seller_id', 
            'seller_zip_code_prefix', 
            'seller_city', 
            'seller_state'
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
    tranlation_df = pd.DataFrame(
        tranlation_data, 
        columns = [
            'product_category_name',
            'product_category_name_english',
        ]
    )
    master_df = orders_df.merge(order_status_df, on='order_id').merge(payments_df, on="order_id") \
    .merge(customers_df, on='customer_id') \
    .merge(product_df, on ="product_id") \
    .merge(tranlation_df, on='product_category_name') \
    .merge(seller_df, on="seller_id")
    return master_df