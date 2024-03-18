import os, sys,re

#Add parent dir and sub dirs to the python path for importing the modules from different directories
currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
rootdir = os.path.dirname(parentdir)
sys.path.extend([rootdir, parentdir])

import pandas as pd


class Transform:
    
    def transform_customer(self):
        customers_df = pd.read_csv("../data/olist_customers_dataset.csv")
        return customers_df

    def transform_geolocation(self):
        geoloc_df = pd.read_csv("../data/olist_geolocation_dataset.csv")
        return geoloc_df

    def transform_orders(self):
        order_df = pd.read_csv("../data/olist_order_items_dataset.csv")
        return order_df

    def transform_payments(self):
        payment_df = pd.read_csv("../data/olist_order_payments_dataset.csv")
        return payment_df

    def transform_reviews(self):
        review_df = pd.read_csv("../data/olist_order_reviews_dataset.csv")
        return review_df

    def transform_order_status(self):
        order_status = pd.read_csv("../data/olist_orders_dataset.csv")
        return order_status

    def transform_products(self):
        product_df = pd.read_csv("../data/olist_products_dataset.csv")
        return product_df
    
    def transform_sellers(self):
        seller_df = pd.read_csv("../data/olist_sellers_dataset.csv")
        return seller_df

    def transform_category_translation(self):
        category_translation = pd.read_csv("../data/product_category_name_translation.csv")
        return category_translation



 