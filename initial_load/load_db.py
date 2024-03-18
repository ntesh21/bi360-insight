import os, sys, re


currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
rootdir = os.path.dirname(parentdir)
sys.path.extend([rootdir, parentdir])

import pandas as pd

import mysql.connector
from database.database_connectivity import tran_database_connector
from initial_load.transform_csv import Transform


class LoadDB:
    def __init__(self):
        self.connection, self.engine = tran_database_connector()
        self.transformer = Transform()

    def create_sql_table_from_df(self, connection, engine, table_name, df):
        try:
            # Insert DataFrame into MySQL table
            df.to_sql(name=table_name, con=engine, if_exists='append', index=False)

            print(f'Data inserted into {table_name} table successfully.')

        except mysql.connector.Error as error:
            print('Error inserting data into MySQL table:', error)

        finally:
            # Close connection
            if connection.is_connected():
                connection.close()
                print('MySQL connection closed.')

    def load_all_tables(self):
        #customer table
        customer_df = self.transformer.transform_customer()
        self.create_sql_table_from_df(self.connection, self.engine, "customers", customer_df)

        #geolocation table
        geolocation_df = self.transformer.transform_geolocation()
        self.create_sql_table_from_df(self.connection, self.engine, "geolocation", geolocation_df)

        #orders table
        orders_df = self.transformer.transform_orders()
        self.create_sql_table_from_df(self.connection, self.engine, "orders", orders_df)

        #payments table
        payments_df = self.transformer.transform_payments()
        self.create_sql_table_from_df(self.connection, self.engine, "payments", payments_df)

        #reviews table
        reviews_df = self.transformer.transform_reviews()
        self.create_sql_table_from_df(self.connection, self.engine, "reviews", reviews_df)

        #order_status table
        order_status_df = self.transformer.transform_order_status()
        self.create_sql_table_from_df(self.connection, self.engine, "order_status", order_status_df)

        #products table
        products_df = self.transformer.transform_products()
        self.create_sql_table_from_df(self.connection, self.engine, "products", products_df)

        #sellers table
        sellers_df = self.transformer.transform_sellers()
        self.create_sql_table_from_df(self.connection, self.engine, "sellers", sellers_df)

        #category_translation table
        category_translation_df = self.transformer.transform_category_translation()
        self.create_sql_table_from_df(self.connection, self.engine, "category_translation", category_translation_df)

        

if __name__ == "__main__":
    loader = LoadDB()
    loader.load_all_tables()


