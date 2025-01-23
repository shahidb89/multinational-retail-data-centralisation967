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
    
if __name__ == '__main__':
    d=DatabaseConnector.init_db_engine()
