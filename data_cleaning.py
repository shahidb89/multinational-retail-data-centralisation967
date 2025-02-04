import pandas as pd
import numpy as np
import re
import data_extraction as dtex
class DataCleaning:
    def clean_user_data():
        user_df = dtex.user_df
        user_df.set_index("index", inplace=True)
        user_df.replace(['NULL'], np.nan, inplace=True)
        user_df["join_date"] = pd.to_datetime(user_df["join_date"], 
                                              format='mixed', errors='coerce')
        user_df = user_df.dropna().reset_index(drop=True)
        return user_df


    def clean_card_data():
        card_df = dtex.card_df
        card_df["date_payment_confirmed"] = pd.to_datetime(card_df["date_payment_confirmed"], 
                                              format='mixed', errors='coerce')
        card_df.replace(['NULL','NaT'], np.nan, inplace=True)
        card_df = card_df.drop_duplicates(subset=['card_number'])
        card_df['card_number'] = pd.to_numeric(card_df['card_number'], errors= 'ignore')
        card_df = card_df.dropna().reset_index(drop=True)

        return card_df
    
    def clean_store_data():
        stores_df = dtex.stores_df
        stores_df= stores_df.drop(columns=['lat'])
        #To keep WEB store row, we have to manually remove None value in latitude column
        stores_df.iloc[0] =[0,'','','','WEB-1388012W','325','2010-06-12 00:00:00','Web Portal','','GB','Europe']
        stores_df["opening_date"] = pd.to_datetime(stores_df["opening_date"], 
                                                   format='mixed', errors='coerce')
        stores_df.loc[stores_df['index'] == 0 , 'latitude'] = ''
        stores_df.replace(['NULL'], np.nan, inplace=True)
        stores_df['staff_numbers'] = stores_df['staff_numbers'].str.replace(r'[^0-9]', '', regex=True)
        
        stores_df = stores_df.dropna().reset_index(drop=True)
        #Avoid empty string in numeric columns before uploading to sales_data database
        stores_df.replace("", pd.NA, inplace=True)
        
        return stores_df
    
    def convert_product_weights():
        products_df = dtex.products_df
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

                # If there is a multiplication (like 12 x 5g), calculate the total weight
                if 'x' in num_part:
                    num_part = eval(num_part.replace('x', '*'))  # Calculate multiplications
                num_part = float(num_part)
                # Convert to kilograms based on the unit
                if unit_part == 'g':
                    num_part *= gram_to_kg  # Convert grams to kilograms
                elif unit_part == 'oz':
                    num_part *= oz_to_kg  # Convert ounces to kilograms
                elif unit_part == 'ml':
                    num_part *= ml_to_kg  # Convert milliliters to kilograms
                elif unit_part == 'kg':
                    pass  # Already in kg, no conversion needed
                else:
                    num_part = "NaN"  # Set to NaN for unknown units    
                if num_part == "NaN":
                    converted_weights.append("NaN")
                # Round the number to two decimal places and append to the list
                else:
                    converted_weights.append(f"{round(float(num_part), 2)}kg")
            else:
                converted_weights.append("NaN")
        products_df['weight'] = converted_weights
        return products_df
    
    def clean_products_data():
        products_df = DataCleaning.convert_product_weights()
        products_df.replace(['NULL', 'NaN'], np.nan, inplace=True)
        products_df = products_df.dropna().reset_index(drop=True)
        return products_df

    def clean_orders_data():
        orders_df = dtex.orders_df
        orders_df= orders_df.drop(columns=['first_name', 'last_name', '1'])
        return orders_df
    
    def clean_events_data():
        events_df = dtex.events_df
        events_df.replace(['NULL'], np.nan, inplace=True)
        events_df['day'] = pd.to_numeric(events_df['day'], errors='coerce', downcast='integer')
        events_df['month'] = pd.to_numeric(events_df['month'], errors='coerce',downcast='integer')
        events_df['year'] = pd.to_numeric(events_df['year'], errors='coerce', downcast='integer')
        events_df = events_df.dropna().reset_index(drop=True)
        return events_df
    

cleaned_user_data = DataCleaning.clean_user_data()
cleaned_card_data = DataCleaning.clean_card_data()
cleaned_stores_data = DataCleaning.clean_store_data()
cleaned_products_data = DataCleaning.clean_products_data()
cleaned_orders_data = DataCleaning.clean_orders_data()
cleaned_events_data = DataCleaning.clean_events_data()
        
if __name__ == '__main__':
    print (DataCleaning.clean_store_data().info())
