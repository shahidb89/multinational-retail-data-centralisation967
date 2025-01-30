import database_utils as dbut
import pandas as pd
from sqlalchemy import inspect
from tabula import read_pdf
import requests
class DataExtractor:
    def list_rds_tables():
        engine = dbut.DatabaseConnector.init_db_engine()
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        return tables
    
    def read_rds_table(table):
        engine = dbut.DatabaseConnector.init_db_engine()
        df = pd.read_sql_table(table, engine)
        return df

    def retrieve_pdf_data(link="https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"):
        dfs = read_pdf(link, pages="all", multiple_tables=True)
        combined_df = pd.concat(dfs, ignore_index=True)
        return combined_df


    def list_number_of_stores(url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores',
                          headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}):   
        response = requests.get(url, headers= headers)
        stores = response.json()
        number_of_stores = stores['number_stores']
        return number_of_stores
    

    def retrieve_sotores_data():
        headers =  {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
        number_of_stores = DataExtractor.list_number_of_stores()
        list_of_stores = []
    
        for i in range (number_of_stores):
            store_url = url + str(i)
            response = requests.get(store_url, headers = headers)
            if response.status_code == 200:
                store = response.json()
                list_of_stores.append(store)
        stores_df = pd.DataFrame(list_of_stores, index=None)
                
        return stores_df


user_df = DataExtractor.read_rds_table("legacy_users")
card_df = DataExtractor.retrieve_pdf_data()
stores_df = DataExtractor.retrieve_sotores_data()
# tables = DataExtractor.list_rds_tables()

# for t in tables:
#     print (t)

if __name__ == "__main__":
    
    print(stores_df)