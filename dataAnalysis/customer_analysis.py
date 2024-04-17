import os, sys
#Add parent dir and sub dirs to the python path for importing the modules from different directories
currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
rootdir = os.path.dirname(parentdir)
sys.path.extend([rootdir, parentdir])

import pandas as pd
import numpy as np
from bi360_insight.database.database_connectivity import SQLWrapper


sql_wrapper = SQLWrapper()

def get_customer_summary():
    customers_data = sql_wrapper.fetch_table_data('customers_dim')
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
    total_customers = len(customers_df.customer_id.unique())
    customer_summary = {"Total Customers": total_customers}
    return customer_summary



def get_city_with_highest_customers():
    customers_data = sql_wrapper.fetch_table_data('customers_dim')
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
    top_city = customers_df.groupby('customer_city')['customer_unique_id'].nunique().sort_values(ascending=False)[:10]
    return top_city.to_dict()


def get_customers_segment_by_lifetime():
   lifetime_segmentation_df = pd.read_csv(f'{currentdir}/lifetime_segmentation.csv')
   lifetime_segments = lifetime_segmentation_df['cluster'].value_counts()
   return lifetime_segments.to_dict()


def get_customers_segment_by_rfm():
    rfm_segmentation_df = pd.read_csv(f'{currentdir}/rfm_segmentation.csv')
    rfm_segmentation = rfm_segmentation_df['Segments'].value_counts().to_dict()
    rfm_segmentation_ratio = {}
    for segment,count in rfm_segmentation.items():
        rfm_segmentation_ratio[segment] = round((count/len(rfm_segmentation_df)) * 100, 2)
    return rfm_segmentation, rfm_segmentation_ratio

def clean_data(data, column_text='review_comment_message', 
               column_score='review_score', 
               points_cut = [0, 2, 5], 
               classes = [0, 1]):
    
    df_bin = data
    df_bin = df_bin.dropna(subset=[column_text])
    df_bin['label'] = pd.cut(df_bin[column_score], bins=points_cut, labels=classes)
    df_bin = df_bin.rename(columns={column_text: 'text'})
    df_bin = df_bin[['text','label']]
    
    df_cat = data
    df_cat = df_cat.dropna(subset=[column_text])
    df_cat = df_cat.rename(columns={column_text: 'text' , column_score: 'label'})
    df_cat = df_cat[['text','label']]
    return df_bin ,df_cat


def get_review_vs_rating():
    reviews_data = sql_wrapper.fetch_table_data('reviews_dim')
    reviews_df = pd.DataFrame(
        reviews_data,
        columns = [
            'review_id',
            'order_id',
            'review_score',
            'review_comment_title',
            'review_comment_message',
            'review_creation_date',
            'review_answer_timestamp'
        ]
    )
    data_bin , data_cat = clean_data(reviews_df)
    review_vs_rating = {}
    labels = np.unique(data_cat["label"])
    for lb in labels:
        review_vs_rating[str(lb)] = list(data_cat["label"]).count(lb)
    return review_vs_rating

