import database_utils as dbut
import pandas as pd
from sqlalchemy import inspect
from tabula import read_pdf
import requests
import boto3
import botocore 
from botocore import UNSIGNED 
from botocore.config import Config 
import os 

class DataExtractor:
    def list_rds_tables():
        engine = dbut.DatabaseConnector.init_db_engine()
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        return tables
    
    # tables = ['legacy_store_details', 'dim_card_details', 'legacy_users', 'orders_table']
    
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


    def extract_from_s3(bucket_name, object_key, local_file_path):
        s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
        s3.download_file(bucket_name, object_key, local_file_path)
        file_extension = os.path.splitext(local_file_path)[1].lower()  # Extract file extension
        if file_extension == '.csv':
            df = pd.read_csv(local_file_path)
        elif file_extension == '.json':
            df = pd.read_json(local_file_path)
        else:
            raise ValueError("File type not supported. Please provide a CSV or JSON file.")
        os.remove(local_file_path)
        return df
    
    

user_df = DataExtractor.read_rds_table("legacy_users")
card_df = DataExtractor.retrieve_pdf_data()
stores_df = DataExtractor.retrieve_sotores_data()
products_df = DataExtractor.extract_from_s3('data-handling-public','products.csv', 
                         '/Users/admin/AiCore/multinational-retail-data/products.csv')
orders_df = DataExtractor.read_rds_table('orders_table')
events_df = DataExtractor.extract_from_s3('data-handling-public','date_details.json', 
                         '/Users/admin/AiCore/multinational-retail-data/date_details.json')


    
   