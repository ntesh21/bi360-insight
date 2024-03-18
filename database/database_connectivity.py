import mysql.connector
from sqlalchemy import create_engine

def tran_database_connector():
    # Establish connection to MySQL database
    host = 'localhost'
    port = 3306
    user = 'root'
    password = 'root'
    database = 'e_commerce_transaction'
    connection = mysql.connector.connect(host=host, user=user, password=password, database=database)
    print(f'mysql://{user}:{password}@{host}:{port}/{database}')
    # Create SQLAlchemy engine
    engine = create_engine(f'mysql://{user}:{password}@{host}:{port}/{database}')
    return connection, engine
