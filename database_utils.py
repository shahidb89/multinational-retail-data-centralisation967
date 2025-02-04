
import yaml
from sqlalchemy import create_engine
import psycopg2

class DatabaseConnector:
    def read_db_creds():
        with open ('db_creds.yaml', 'r') as db:
            db_creds_dict = yaml.load(db,Loader=yaml.SafeLoader)
        return db_creds_dict
    

    def init_db_engine():
        db_cred_dict =  DatabaseConnector.read_db_creds()
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        ENDPOINT = db_cred_dict['RDS_HOST']
        USER = db_cred_dict['RDS_USER']
        PASSWORD = db_cred_dict['RDS_PASSWORD']
        PORT = db_cred_dict['RDS_PORT']
        DATABASE = db_cred_dict['RDS_DATABASE']
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}")
        engine.connect()
        return engine
    

    def write_db_creds():
        with open ('sales_db_creds.yaml', 'r') as db:
            db_creds_dict = yaml.load(db,Loader=yaml.SafeLoader)
        return db_creds_dict
    
    def write_db_engine():
        db_cred_dict =  DatabaseConnector.write_db_creds()
        DATABASE_TYPE = db_cred_dict['DATABASE_TYPE']
        DBAPI = db_cred_dict['DBAPI']
        HOST = db_cred_dict['HOST']
        USER = db_cred_dict['USER']
        PASSWORD = db_cred_dict['PASSWORD']
        DATABASE = db_cred_dict['DATABASE']
        PORT = db_cred_dict['PORT']
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return engine
    
    def upload_to_db(df_name, table_name):
        engine = DatabaseConnector.write_db_engine()
        df_name.to_sql(table_name, con=engine, if_exists='replace', index=False)
        print(f'{table_name} table uploaded to sales_data database successfully.')
    
    def write_db_connector():
        db_creds_dict = DatabaseConnector.write_db_creds()
        conn = psycopg2.connect(
                dbname = db_creds_dict['DATABASE'],
                user =  db_creds_dict['USER'],
                password = db_creds_dict['PASSWORD'],
                host = db_creds_dict['HOST'],
                port = db_creds_dict['PORT']
                )  

        return conn 
    
    def execute_sql(query):
        try:
            conn = DatabaseConnector.write_db_connector()
            cursor = conn.cursor()
            cursor.execute(query)
            if query.strip().upper().startswith(("INSERT", "UPDATE", "DELETE", "ALTER")):
                conn.commit()
            print ("Query executed successfully!")

        except Exception as e:
            print("Error:", e)
            conn.rollback()
        
        finally:
            cursor.close()
            conn.close()




if __name__ == '__main__':
    import data_cleaning
    DatabaseConnector.upload_to_db(data_cleaning.cleaned_user_data, 'dim_users')
    DatabaseConnector.upload_to_db(data_cleaning.cleaned_card_data, 'dim_card_details')
    DatabaseConnector.upload_to_db(data_cleaning.cleaned_stores_data, 'dim_store_details')
    DatabaseConnector.upload_to_db(data_cleaning.cleaned_products_data, 'dim_products')
    DatabaseConnector.upload_to_db(data_cleaning.cleaned_orders_data, 'order_table')
    DatabaseConnector.upload_to_db(data_cleaning.cleaned_events_data, 'dim_date_times')


    

