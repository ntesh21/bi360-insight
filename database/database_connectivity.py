import mysql.connector
from sqlalchemy import create_engine



class SQLWrapper:
    def __init__(self):
        self.host = 'localhost'
        self.port = 3306
        self.user = 'root'
        self.password = 'root'
        self.auth_plugin='mysql_native_password'

    def mySql_connection(self, db_name = None):
        if db_name is None:
            conn = mysql.connector.connect(
                host = self.host,
                port = self.port,
                user = self.user,
                password = self.password,
                auth_plugin = self.auth_plugin
            )
        else:
            conn = mysql.connector.connect(
            host = self.host,
            port = self.port,
            user = self.user,
            password = self.password,
            database = db_name,
            auth_plugin = self.auth_plugin
        )
        return conn
    
    def create_sqlalchemy_engine(self, database):
        connection = mysql.connector.connect(host=self.host, user=self.user, password=self.password, database=database)
        print(f'mysql://{self.user}:{self.password}@{self.host}:{self.port}/{database}')
        # Create SQLAlchemy engine
        engine = create_engine(f'mysql://{self.user}:{self.password}@{self.host}:{self.port}/{database}')
        return connection, engine


    def create_db(self, db_name):
        connection = self.mySql_connection()
        cursor = connection.cursor()
        # Define SQL query to create database
        create_db_query = "CREATE DATABASE IF NOT EXISTS {}".format(db_name)
        # Execute SQL query to create database
        cursor.execute(create_db_query)
        print("Database Created Successfully")
        # Close cursor and connection
        cursor.close()
        connection.close()
        

    def query_operation(self, warehouse_name, create_table_query):
        connection = self.mySql_connection(db_name=warehouse_name)
        cursor = connection.cursor()
        cursor.execute(create_table_query)
        # Commit changes to the database
        connection.commit()
        # Close cursor and connection
        cursor.close()
        connection.close()

    def create_sql_table_from_df(self, engine, table_name, df):
        try:
            # Insert DataFrame into MySQL table
            df.to_sql(name=table_name, con=engine, if_exists='append', index=False)

            print(f'Data inserted into {table_name} table successfully.')

        except mysql.connector.Error as error:
            print('Error inserting data into MySQL table:', error)
    
    def fetch_table_data(self, table_name):
        connection = self.mySql_connection('BI360')
        cursor = connection.cursor()
        query = f"""
                    SELECT * FROM {table_name};
                """
        cursor.execute(query)
        result = cursor.fetchall()
        return result

    # def insert_table(self, warehouse_name, insert_table_query):
    #     connection = self.mySql_connection(db_name=warehouse_name)
    #     cursor = connection.cursor()
    #     cursor.execute(insert_table_query)
    #     # Commit changes to the database
    #     connection.commit()
    #     # Close cursor and connection
    #     cursor.close()
    #     connection.close()



