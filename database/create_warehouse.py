from database_connectivity import SQLWrapper
import json
import pandas as pd


class BIwarehouse:
    def __init__(self, warehouse_name):
        self.sql_wrapper = SQLWrapper()
        self.warehouse_name = warehouse_name
        with open('query_mapper.json', 'r') as file:
            self.query_mapper = json.load(file)
          
    def load_df(self, filename):
        return pd.read_csv(filename)

    def __create_db(self):
        return self.sql_wrapper.create_db(self.warehouse_name)
    
    def __create_tables(self):
        for _,v in self.query_mapper.items():
            # try:
            self.sql_wrapper.query_operation(self.warehouse_name, v)
            # except:
                # continue
        return "All tables created successfully."
    
    def insert_into_table(self, table_name, val_data):
        for data in val_data:
            try:
                query = f"""INSERT INTO {table_name} VALUES {"('"+"', '".join(data)+"')"};"""
                self.sql_wrapper.query_operation(self.warehouse_name, query)
            except:
                continue
        print(f"Data inserted in {table_name} successfully.")
        # return self.sql_wrapper.query_operation(self.warehouse_name, query)
    
    # def insert_from_(self, df, table_name):
    #     col_names = list(df.columns)
    #     val_data = list(df.itertuples(index=False, name=None))
    #     val_data = [tuple(str(d) for d in data) for data in val_data]
    #     self.insert_into_table(table_name, col_names, val_data)

    def insert_from_df(self, table_name, df):
        _,engine = self.sql_wrapper.create_sqlalchemy_engine(self.warehouse_name)
        self.sql_wrapper.create_sql_table_from_df(engine, table_name, df)
        print("Data Inserted Successfully")

    def insert_customer_data(self):
        table_name = "customers_dim"
        df = self.load_df('../data/olist_customers_dataset.csv')
        return self.insert_from_df(table_name, df)
    
    def insert_products_data(self):
        table_name = "products_dim"
        df = self.load_df('../data/olist_products_dataset.csv')
        df = df.dropna()
        return self.insert_from_df(table_name, df)
    
    def insert_sellers_data(self):
        table_name = "sellers_dim"
        df = self.load_df('../data/olist_sellers_dataset.csv')
        return self.insert_from_df(table_name, df)
    
    def insert_orders_data(self):
        table_name = "orders_fact"
        df = self.load_df('../data/olist_order_items_dataset.csv')
        return self.insert_from_df(table_name, df)
    
    def insert_reviews_data(self):
        table_name = "reviews_dim"
        df = self.load_df('../data/olist_order_reviews_dataset.csv')
        return self.insert_from_df(table_name, df)
    
    def insert_payments_data(self):
        table_name = "payments_dim"
        df = self.load_df('../data/olist_order_payments_dataset.csv')
        return self.insert_from_df(table_name, df)
    
    def insert_order_status_data(self):
        table_name = "order_status_dim"
        df = self.load_df('../data/olist_orders_dataset.csv')
        return self.insert_from_df(table_name, df)
    
    def insert_category_traslation(self):
        table_name = "product_translation_dim"
        df = self.load_df('../data/product_category_name_translation.csv')
        return self.insert_from_df(table_name, df)
    
    def insert_geolocation_data(self):
        table_name = "geolocation_dim"
        df = self.load_df('../data/olist_geolocation_dataset.csv')
        return self.insert_from_df(table_name, df)
    
    

    def create_warehouse(self):
        self.__create_db()
        # self.__create_tables()
        
    
if __name__ == '__main__':
    wh = BIwarehouse('BI360')
    wh.create_warehouse()
    wh.insert_customer_data()
    wh.insert_products_data()
    wh.insert_sellers_data()
    wh.insert_orders_data()
    wh.insert_reviews_data()
    wh.insert_payments_data()
    wh.insert_order_status_data()
    wh.insert_category_traslation()
    wh.insert_geolocation_data

    

        

    
    






