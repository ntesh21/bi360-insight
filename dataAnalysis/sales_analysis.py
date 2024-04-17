import os, sys
#Add parent dir and sub dirs to the python path for importing the modules from different directories
currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
rootdir = os.path.dirname(parentdir)
sys.path.extend([rootdir, parentdir])

import pandas as pd

def get_sales_summary(status_df):
    sales_fmt = status_df.copy()
    sales_fmt["order_purchase_timestamp"] = pd.to_datetime(status_df["order_purchase_timestamp"], format='%Y-%m-%d %H:%M:%S')
    sales_fmt["order_delivered_carrier_date"] = pd.to_datetime(status_df["order_delivered_carrier_date"], format='%Y-%m-%d %H:%M:%S')
    sales_fmt["order_delivered_customer_date"] = pd.to_datetime(status_df["order_delivered_customer_date"], format='%Y-%m-%d %H:%M:%S')
    sales_fmt["order_estimated_delivery_date"] = pd.to_datetime(status_df["order_estimated_delivery_date"], format='%Y-%m-%d %H:%M:%S')
    sales_fmt['month'] = sales_fmt['order_purchase_timestamp'].dt.strftime('%b%Y')
    sales_figures = sales_fmt.groupby('month').size().sort_values().to_dict()
    sales_summary = {}
    sales_summary["Average sales per Month"] = round(sum(list(sales_figures.values()))/len(list(sales_figures.values())), 2)
    sales_summary["Month with highest Sales"] = max(sales_figures, key=sales_figures.get)
    sales_summary["Month with lowest Sales"] = min(sales_figures, key=sales_figures.get)
    return sales_summary


def get_city_wise_sellers(seller_df):
    seller_city = seller_df["seller_city"].value_counts().sort_values(ascending=False)[:10]
    city_wise_seller = seller_city.to_dict()
    return city_wise_seller
    

def get_product_sold_per_category(products_df):
    top_categories = products_df[['product_category_name_english', 'order_item_id']]
    top_categories = top_categories.groupby(['product_category_name_english']).sum().sort_values(by=['order_item_id'], ascending=False).reset_index()
    top_category = top_categories[:10]
    bottom_category = top_categories[-10:]
    return dict(zip(top_category.product_category_name_english, top_category.order_item_id)), dict(zip(bottom_category.product_category_name_english, bottom_category.order_item_id))

    

def get_payment_types(payments_df):
    return payments_df.groupby(['payment_type']).size().to_dict()


def get_average_frieght_by_product_category(master_df):
    MeanSales_cat = master_df.groupby(["product_category_name_english"]).agg({"price":"mean", "freight_value":"mean"})
    MeanSales_cat = MeanSales_cat.sort_values(ascending=False, by="price")[:10].reset_index()
    avg_frieght = MeanSales_cat[['product_category_name_english', 'freight_value']]
    return dict(zip(avg_frieght.product_category_name_english, avg_frieght.freight_value))

