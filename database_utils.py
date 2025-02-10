import yaml
from sqlalchemy import create_engine
import psycopg2
import pandas as pd

class DatabaseConnector:
    """
    A class to manage database connections, credentials, and SQL executions.
    """

    @staticmethod
    def read_db_creds():
        """
        Reads database credentials from the `db_creds.yaml` file.
        
        Returns:
            dict: A dictionary containing database credentials.
        """
        with open('db_creds.yaml', 'r') as db:
            db_creds_dict = yaml.load(db, Loader=yaml.SafeLoader)
        return db_creds_dict
    
    @staticmethod
    def init_db_engine():
        """
        Initializes and returns a SQLAlchemy database engine for reading data.
        
        Returns:
            sqlalchemy.engine.base.Engine: A SQLAlchemy engine instance.
        """
        db_cred_dict = DatabaseConnector.read_db_creds()
        
        # Extract credentials
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        ENDPOINT = db_cred_dict['RDS_HOST']
        USER = db_cred_dict['RDS_USER']
        PASSWORD = db_cred_dict['RDS_PASSWORD']
        PORT = db_cred_dict['RDS_PORT']
        DATABASE = db_cred_dict['RDS_DATABASE']
        
        # Create database engine
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}")
        engine.connect()
        return engine
    
    @staticmethod
    def write_db_creds():
        """
        Reads database credentials for writing from the `sales_db_creds.yaml` file.
        
        Returns:
            dict: A dictionary containing database credentials for writing.
        """
        with open('sales_db_creds.yaml', 'r') as db:
            db_creds_dict = yaml.load(db, Loader=yaml.SafeLoader)
        return db_creds_dict
    
    @staticmethod
    def write_db_engine():
        """
        Initializes and returns a SQLAlchemy database engine for writing data.
        
        Returns:
            sqlalchemy.engine.base.Engine: A SQLAlchemy engine instance.
        """
        db_cred_dict = DatabaseConnector.write_db_creds()
        
        # Extract credentials
        DATABASE_TYPE = db_cred_dict['DATABASE_TYPE']
        DBAPI = db_cred_dict['DBAPI']
        HOST = db_cred_dict['HOST']
        USER = db_cred_dict['USER']
        PASSWORD = db_cred_dict['PASSWORD']
        DATABASE = db_cred_dict['DATABASE']
        PORT = db_cred_dict['PORT']
        
        # Create database engine
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return engine
    
    @staticmethod
    def upload_to_db(df_name, table_name):
        """
        Uploads a Pandas DataFrame to the database.
        
        Args:
            df_name (pd.DataFrame): The DataFrame to be uploaded.
            table_name (str): The target table name in the database.
        """
        engine = DatabaseConnector.write_db_engine()
        df_name.to_sql(table_name, con=engine, if_exists='replace', index=False)
        print(f'{table_name} table uploaded to sales_data database successfully.')
    
    @staticmethod
    def write_db_connector():
        """
        Establishes and returns a direct database connection using psycopg2.
        
        Returns:
            psycopg2.extensions.connection: A PostgreSQL database connection.
        """
        db_creds_dict = DatabaseConnector.write_db_creds()
        conn = psycopg2.connect(
            dbname=db_creds_dict['DATABASE'],
            user=db_creds_dict['USER'],
            password=db_creds_dict['PASSWORD'],
            host=db_creds_dict['HOST'],
            port=db_creds_dict['PORT']
        )  
        return conn 
    
    @staticmethod
    def execute_sql(query):
        """
        Executes an SQL query and returns results if applicable.
        
        Args:
            query (str): The SQL query to be executed.
        
        Returns:
            pd.DataFrame: Query results in a Pandas DataFrame (for SELECT queries).
            None: For queries that modify data (INSERT, UPDATE, DELETE, ALTER).
        """
        query_type = query.strip().upper().split()[0]
        
        if query_type in ("INSERT", "UPDATE", "DELETE", "ALTER"):  # Queries that modify data
            try:
                conn = DatabaseConnector.write_db_connector()
                cursor = conn.cursor()
                cursor.execute(query)
                conn.commit()
                print("Query executed successfully!")
            except Exception as e:
                print("Error:", e)
                conn.rollback()
            finally:
                cursor.close()
                conn.close()
        else:  # Queries that retrieve data
            engine = DatabaseConnector.write_db_engine()
            return pd.read_sql_query(query, engine)




if __name__ == '__main__':
    import data_cleaning
    DatabaseConnector.upload_to_db(data_cleaning.DataCleaning.clean_user_data(), 'dim_users')
    DatabaseConnector.upload_to_db(data_cleaning.DataCleaning.clean_card_data(), 'dim_card_details')
    DatabaseConnector.upload_to_db(data_cleaning. DataCleaning.clean_store_data(), 'dim_store_details')
    DatabaseConnector.upload_to_db(data_cleaning.DataCleaning.clean_products_data(), 'dim_products')
    DatabaseConnector.upload_to_db(data_cleaning.DataCleaning.clean_orders_data(), 'order_table')
    DatabaseConnector.upload_to_db(data_cleaning.DataCleaning.clean_events_data(), 'dim_date_times')
