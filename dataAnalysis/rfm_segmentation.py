import os, sys
#Add parent dir and sub dirs to the python path for importing the modules from different directories
currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
rootdir = os.path.dirname(parentdir)
sys.path.extend([rootdir, parentdir])


from bi360_insight.database.database_connectivity import SQLWrapper

import pandas as pd
import numpy as np
from sklearn.impute import SimpleImputer
from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import IterativeImputer

sql_wrapper = SQLWrapper()

def get_customers_segment_by_rfm():
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
    orders_data = sql_wrapper.fetch_table_data('orders_fact')
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
    order_status_data = sql_wrapper.fetch_table_data('order_status_dim')
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
    sellers_data = sql_wrapper.fetch_table_data('sellers_dim')
    sellers_df = pd.DataFrame(
        sellers_data, 
        columns=[
            'seller_id', 
            'seller_zip_code_prefix', 
            'seller_city', 
            'seller_state'
        ]
    )
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
    product_data = sql_wrapper.fetch_table_data('products_dim')
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
    geolocation_df = pd.read_csv(f'{parentdir}/data/olist_geolocation_dataset.csv')
    data_train = order_status_df.merge(customers_df, on="customer_id").merge(orders_df, on="order_id").merge(product_df, on="product_id").merge(payments_df, on="order_id").merge(sellers_df, on="seller_id").merge(reviews_df, on="order_id")

    geolocation_df['customer_zip_code_prefix'] = geolocation_df['geolocation_zip_code_prefix']
    geol = geolocation_df.groupby(['customer_zip_code_prefix'],as_index=False).agg({
        'geolocation_lat':'mean',
        'geolocation_lng':'mean'
    })
    data_train = data_train.merge(geol,how='left',on='customer_zip_code_prefix')
    data_missing_columns = data_train.columns.drop([
         'review_comment_title', 
         "review_comment_message", 
         'order_id', 
         'customer_id', 
         'product_id',
         'customer_unique_id', 
         'order_item_id', 
         'seller_id', 
         'review_id'
        ]
    )
    # select numerical columns 
    numerical_col_train = data_train[data_missing_columns].select_dtypes(include=[np.number]).columns
    # Numercial missing value imputation
    imputer  = IterativeImputer(max_iter=25, random_state=0)
    imputed = imputer.fit_transform(data_train[numerical_col_train])
    data_train[numerical_col_train] = pd.DataFrame(imputed, columns=numerical_col_train)
    # # Selecting the categorical variables
    categorical_col_train = data_train[data_missing_columns].select_dtypes(exclude=[np.number]).columns
    # Categorical missing value imputation
    imputerSimple = SimpleImputer(missing_values=np.NaN, strategy='most_frequent')
    data_train[categorical_col_train] = imputerSimple.fit_transform(data_train[categorical_col_train])
    imputerSimpleReview = SimpleImputer(strategy='constant', fill_value='')
    data_train.loc[:,['review_comment_title', 'review_comment_message']] = imputerSimpleReview.fit_transform(data_train.loc[:,['review_comment_title', 'review_comment_message']])
    # missing_values(data_train)
    time_sol = ['order_purchase_timestamp','order_approved_at', 'order_delivered_carrier_date',
       'order_delivered_customer_date', 'order_estimated_delivery_date','shipping_limit_date',
       'review_creation_date', 'review_answer_timestamp',]
    #changing the date object to pandas time format
    for t in time_sol:
        data_train[t] =  pd.to_datetime(data_train[t], format='%Y-%m-%d %H:%M:%S')
    for col in data_train.select_dtypes(include=["float64"]).columns:
        q1_col = data_train[col].quantile(0.025)
        q3_col = data_train[col].quantile(0.975)
        data_train[col] = np.where((data_train[col] > q3_col), q3_col, data_train[col])
    data_train['delivery_days'] = (data_train['order_estimated_delivery_date']-data_train['order_delivered_customer_date']).dt.days
    data_train['late_delivery'] = np.where(data_train['delivery_days'] >= 0, 0, 1)
    data_train['freight_ratio'] = data_train['freight_value'] / data_train['payment_value']
    data_train['total_order_value'] = data_train['payment_value'] + data_train['freight_value']
    data_train['order_weekday'] = data_train['order_purchase_timestamp'].dt.weekday
    data_train['approval_time_min'] = (data_train['order_approved_at']-data_train['order_purchase_timestamp']).dt.total_seconds()/60
    data_train['review_comment_message'] = pd.Series(['' if x==None else x for x in data_train['review_comment_message']])
    data_train['length_reviews_comment'] =  data_train['review_comment_message'].map(len)
    data_train['count_comment'] = pd.Series([1 if x >0 else 0 for x in data_train['review_comment_message'].map(len)])
    data_train['number_reviews'] = data_train.groupby('customer_unique_id').count_comment.transform('nunique') 
    data_train.drop('count_comment', axis=1, inplace=True)
    hours_bins = [-0.1, 6, 12, 18, 23]
    hours_labels = ['Sunrise', 'Morning', 'Afternoon', 'Night']
    data_train['purchase_time_day'] = pd.cut(data_train['order_purchase_timestamp'].apply(lambda x: x.hour), hours_bins, labels=hours_labels)
    data_train.drop(['order_approved_at', 'order_delivered_carrier_date','order_delivered_customer_date', 'order_estimated_delivery_date'], axis=1, inplace=True)
    cat_attributes = ['order_status', 'payment_type', 'purchase_time_day']
    data_train = pd.get_dummies(data_train, columns = cat_attributes)
    # Recency 
    data_rfm = data_train.groupby(by='customer_unique_id', as_index=False)['order_purchase_timestamp'].max()
    recent_date = data_train['order_purchase_timestamp'].dt.date.max() # Recent date of order
    data_rfm['Recency'] = data_rfm["order_purchase_timestamp"].dt.date.apply(lambda x: (recent_date - x).days)
    data_rfm

    # Frequency
    data_rfm['Frequency'] = data_train.groupby('customer_unique_id')['order_id'].transform('nunique')

    # Monetry 
    data_rfm['Monetary'] = data_train.groupby('customer_unique_id', as_index=False)['payment_value'].sum()['payment_value']
    data_rfm["recency_score"]  = pd.qcut(data_rfm['Recency'], 5, labels=[5, 4, 3, 2, 1])
    data_rfm["frequency_score"]= pd.qcut(data_rfm['Frequency'].rank(method="first"), 5, labels=[1, 2, 3, 4, 5])
    data_rfm["monetary_score"] = pd.qcut(data_rfm['Monetary'], 5, labels=[1, 2, 3, 4, 5])

    data_rfm['score_rfm'] = data_rfm.recency_score.astype(str)+ data_rfm.frequency_score.astype(str) + data_rfm.monetary_score.astype(str)
    seg_map= {
        r'111|112|121|131|141|151': 'Lost customers',
        r'332|322|233|232|223|222|132|123|122|212|211': 'Hibernating customers', 
        r'155|154|144|214|215|115|114|113': 'Cannot Lose Them',
        r'255|254|245|244|253|252|243|242|235|234|225|224|153|152|145|143|142|135|134|133|125|124': 'At Risk',
        r'331|321|312|221|213|231|241|251': 'About To Sleep',
        r'535|534|443|434|343|334|325|324': 'Need Attention',
        r'525|524|523|522|521|515|514|513|425|424|413|414|415|315|314|313': 'Promising',
        r'512|511|422|421|412|411|311': 'New Customers',
        r'553|551|552|541|542|533|532|531|452|451|442|441|431|453|433|432|423|353|352|351|342|341|333|323': 'Potential Loyalist',
        r'543|444|435|355|354|345|344|335': 'Loyal',
        r'555|554|544|545|454|455|445': 'Champions'
    }
    data_rfm['Segments'] = data_rfm['recency_score'].astype(str) + data_rfm['frequency_score'].astype(str) + data_rfm['monetary_score'].astype(str)
    data_rfm['Segments'] = data_rfm['Segments'].replace(seg_map, regex=True)
    data_rfm.groupby('Segments', as_index=False).agg({
    'Recency':'mean',
    'Frequency':'mean',
    'Monetary': "mean"
    })
    data_rfm.to_csv('rfm_segmentation.csv')


if __name__ == '__main__':
    get_customers_segment_by_rfm()