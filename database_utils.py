import yaml

class DatabaseConnector:
    def read_db_creds():
        with open ('db_creds.yaml', 'r') as db:
            db_creds_dict = yaml.load(db,Loader=yaml.SafeLoader)
        return db_creds_dict
    



