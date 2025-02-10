import pandas as pd
import numpy as np
import re
import data_extraction as dtex
import os

class DataCleaning:
    """
    A class containing methods for cleaning various datasets used in a retail business.
    """

    def clean_user_data():
        """
        Cleans user data by:
        - Reading data from an RDS table
        - Replacing 'NULL' values with NaN
        - Converting 'join_date' column to datetime
        - Dropping rows with missing values
        
        Returns:
            pd.DataFrame: Cleaned user data
        """
        user_df = dtex.DataExtractor.read_rds_table("legacy_users")
        user_df.set_index("index", inplace=True)
        user_df.replace(['NULL'], np.nan, inplace=True)
        user_df["join_date"] = pd.to_datetime(user_df["join_date"], format='mixed', errors='coerce')
        user_df = user_df.dropna().reset_index(drop=True)
        return user_df

    def clean_card_data():
        """
        Cleans card data by:
        - Reading data from a PDF
        - Converting 'date_payment_confirmed' to datetime
        - Replacing 'NULL' and 'NaT' values with NaN
        - Dropping duplicate card numbers
        - Converting 'card_number' column to numeric where possible
        - Dropping rows with missing values
        
        Returns:
            pd.DataFrame: Cleaned card data
        """
        card_df = dtex.DataExtractor.retrieve_pdf_data()
        card_df["date_payment_confirmed"] = pd.to_datetime(card_df["date_payment_confirmed"], format='mixed', errors='coerce')
        card_df.replace(['NULL', 'NaT'], np.nan, inplace=True)
        card_df = card_df.drop_duplicates(subset=['card_number'])
        card_df['card_number'] = pd.to_numeric(card_df['card_number'], errors='ignore')
        card_df = card_df.dropna().reset_index(drop=True)
        return card_df
    
    def clean_store_data():
        """
        Cleans store data by:
        - Dropping the 'lat' column
        - Manually fixing the first row for 'WEB' store
        - Converting 'opening_date' to datetime
        - Removing non-numeric characters from 'staff_numbers'
        
        Returns:
            pd.DataFrame: Cleaned store data
        """
        stores_df = dtex.DataExtractor.retrieve_stores_data()
        stores_df = stores_df.drop(columns=['lat'])
        # Manually fixing the first row for WEB store
        stores_df.iloc[0] = [0, '', '', '', 'WEB-1388012W', '325', '2010-06-12 00:00:00', 'Web Portal', '', 'GB', 'Europe']
        stores_df["opening_date"] = pd.to_datetime(stores_df["opening_date"], format='mixed', errors='coerce')
        stores_df.loc[stores_df['index'] == 0, 'latitude'] = ''
        stores_df.replace(['NULL'], np.nan, inplace=True)
        stores_df['staff_numbers'] = stores_df['staff_numbers'].str.replace(r'[^0-9]', '', regex=True)
        stores_df = stores_df.dropna().reset_index(drop=True)
        # Avoid passing empty string in a nimber column to the sales_data database.
        stores_df.replace("", pd.NA, inplace=True)
        return stores_df
    
    def convert_product_weights():
        """
        Converts product weights to a uniform unit (kilograms).
        - Parses weight values and converts grams, ounces, and milliliters to kilograms.
        - Handles cases where weights are given in multiplication format (e.g., '12 x 5g').
        - Sets invalid weights to NaN.
        
        Returns:
            pd.DataFrame: Dataframe with converted weight values.
        """
        current_folder = os.getcwd()
        products_df = dtex.DataExtractor.extract_from_s3('data-handling-public', 'products.csv', 
                                                         current_folder+'/products.csv')
        weights_list = list(products_df['weight'])
        gram_to_kg = 0.001
        oz_to_kg = 0.0283495
        ml_to_kg = 0.001
        converted_weights = []
        
        for item in weights_list:
            match = re.match(r'([\d\s.x]+)([a-zA-Z]+)', str(item))  # Extract numbers and units
            if match:
                num_part = match.group(1).strip()
                unit_part = match.group(2).strip()
                
                if 'x' in num_part:
                    num_part = eval(num_part.replace('x', '*'))  # Compute multiplication
                num_part = float(num_part)
                
                if unit_part == 'g':
                    num_part *= gram_to_kg  # Convert grams to kilograms
                elif unit_part == 'oz':
                    num_part *= oz_to_kg  # Convert ounces to kilograms
                elif unit_part == 'ml':
                    num_part *= ml_to_kg  # Convert milliliters to kilograms
                elif unit_part == 'kg':
                    pass  # Already in kg, no conversion needed
                else:
                    num_part = "NaN"  # Unknown units
                
                converted_weights.append(f"{round(float(num_part), 2)}kg" if num_part != "NaN" else "NaN")
            else:
                converted_weights.append("NaN")
        
        products_df['weight'] = converted_weights
        return products_df
    
    def clean_products_data():
        """
        Cleans product data by:
        - Converting product weights
        - Replacing 'NULL' and 'NaN' with NaN values
        - Dropping rows with missing values
        
        Returns:
            pd.DataFrame: Cleaned product data
        """
        products_df = DataCleaning.convert_product_weights()
        products_df.replace(['NULL', 'NaN'], np.nan, inplace=True)
        products_df = products_df.dropna().reset_index(drop=True)
        return products_df
    
    def clean_orders_data():
        """
        Cleans orders data by:
        - Removing unnecessary columns ('first_name', 'last_name', '1')
        
        Returns:
            pd.DataFrame: Cleaned orders data
        """
        orders_df = dtex.DataExtractor.read_rds_table('orders_table')
        orders_df = orders_df.drop(columns=['first_name', 'last_name', '1'])
        return orders_df
    
    def clean_events_data():
        """
        Cleans events data by:
        - Replacing 'NULL' values with NaN
        - Converting 'day', 'month', and 'year' to numeric values
        - Dropping rows with missing values
        
        Returns:
            pd.DataFrame: Cleaned events data
        """
        current_folder = os.getcwd()
        events_df = dtex.DataExtractor.extract_from_s3('data-handling-public', 'date_details.json', 
                                                        current_folder+'/date_details.json')
        events_df.replace(['NULL'], np.nan, inplace=True)
        events_df['day'] = pd.to_numeric(events_df['day'], errors='coerce', downcast='integer')
        events_df['month'] = pd.to_numeric(events_df['month'], errors='coerce', downcast='integer')
        events_df['year'] = pd.to_numeric(events_df['year'], errors='coerce', downcast='integer')
        events_df = events_df.dropna().reset_index(drop=True)
        return events_df

    


if __name__ == '__main__':
    print (DataCleaning.clean_products_data().info())
