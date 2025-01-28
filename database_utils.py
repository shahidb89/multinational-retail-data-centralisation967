import data_cleaning
import yaml
from sqlalchemy import create_engine

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
        print(f'{df_name} uploaded to sales_data database successfully.')
    
if __name__ == '__main__':
    user_df = data_cleaning.DataCleaning.clean_user_data()
    DatabaseConnector.upload_to_db(user_df, 'dim_users')
