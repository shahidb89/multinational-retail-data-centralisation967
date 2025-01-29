import pandas as pd
import numpy as np
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
        
       
        card_df["date_payment_confirmed"] = pd.to_datetime(card_df["date_payment_confirmed"], 
                                              format='mixed', errors='coerce')
        card_df = card_df.dropna().reset_index(drop=True)

        return card_df
        
if __name__ == "__main__":
    df = DataCleaning.clean_card_data()
    print(df.info())