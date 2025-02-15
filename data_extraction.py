from botocore import UNSIGNED 
from botocore.config import Config 
from sqlalchemy import inspect
from tabula import read_pdf
import database_utils as dbut
import endpoint_configs
import pandas as pd
import requests
import boto3
import botocore 

import os 

class DataExtractor:
    """
    A class to extract data from various sources, including RDS databases, PDFs, APIs, and AWS S3.
    """
    
    @staticmethod
    def list_rds_tables():
        """
        Lists all table names in the RDS database.

        Returns:
            list: A list of table names in the RDS database.
        """
        engine = dbut.DatabaseConnector.init_db_engine()
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        return tables
    
    @staticmethod
    def read_rds_table(table):
        """
        Reads a specified table from the RDS database and returns it as a DataFrame.
        
        Args:
            table (str): The name of the table to read.
        
        Returns:
            pd.DataFrame: A Pandas DataFrame containing the table data.
        """
        engine = dbut.DatabaseConnector.init_db_engine()
        df = pd.read_sql_table(table, engine)
        return df

    @staticmethod
    def retrieve_pdf_data(link=endpoint_configs.card_pdf_link):
        """
        Extracts tabular data from a PDF file using Tabula.
        
        Args:
            link (str, optional): URL of the PDF file. Defaults to the provided link.
        
        Returns:
            pd.DataFrame: A concatenated DataFrame containing all tables from the PDF.
        """
        dfs = read_pdf(link, pages="all", multiple_tables=True)
        combined_df = pd.concat(dfs, ignore_index=True)
        return combined_df

    @staticmethod
    def list_number_of_stores(url= endpoint_configs.number_of_stores_url,
                              headers= endpoint_configs.number_of_stores_headers):   
        """
        Retrieves the total number of stores from an API endpoint.
        
        Args:
            url (str, optional): The API endpoint for store count. Defaults to the provided URL.
            headers (dict, optional): The API headers containing authentication. Defaults to the provided headers.
        
        Returns:
            int: The number of stores.
        """
        response = requests.get(url, headers=headers)
        stores = response.json()
        number_of_stores = stores['number_stores']
        return number_of_stores
    
    @staticmethod
    def retrieve_stores_data():
        """
        Retrieves store details from an API for all available stores.
        
        Returns:
            pd.DataFrame: A DataFrame containing store details.
        """
        headers = endpoint_configs.stores_data_headers
        url = endpoint_configs.stores_data_url
        number_of_stores = DataExtractor.list_number_of_stores()
        list_of_stores = []
    
        # Loop through all store IDs and fetch their details
        for i in range(number_of_stores):
            store_url = url + str(i)
            response = requests.get(store_url, headers=headers)
            if response.status_code == 200:
                store = response.json()
                list_of_stores.append(store)
        
        stores_df = pd.DataFrame(list_of_stores, index=None)
        return stores_df

    @staticmethod
    def extract_from_s3(bucket_name, object_key, local_file_path):
        """
        Downloads a file from an S3 bucket and loads it into a Pandas DataFrame.
        
        Args:
            bucket_name (str): The name of the S3 bucket.
            object_key (str): The key (path) of the object in the S3 bucket.
            local_file_path (str): The local path to save the downloaded file.
        
        Returns:
            pd.DataFrame: A DataFrame containing the extracted data.
        
        Raises:
            ValueError: If the file type is unsupported.
        """
        s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
        s3.download_file(bucket_name, object_key, local_file_path)
        
        # Determine file type and read accordingly
        file_extension = os.path.splitext(local_file_path)[1].lower()
        if file_extension == '.csv':
            df = pd.read_csv(local_file_path)
        elif file_extension == '.json':
            df = pd.read_json(local_file_path)
        else:
            raise ValueError("File type not supported. Please provide a CSV or JSON file.")
        
        os.remove(local_file_path)  # Clean up by removing the local file
        return df

    



if __name__ == "__main__":
   print (DataExtractor.list_number_of_stores())


    
   