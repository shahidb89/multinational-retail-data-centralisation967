import pandas as pd
import numpy as np
import data_extraction as dtex
class DataCleaning:
    def clean_user_data():
        user_df = dtex.user_df
        user_df.set_index("index", inplace=True)
        user_df.replace(['NULL'], np.nan, inplace=True)
        user_df["join_date"] = pd.to_datetime(user_df["join_date"], format='mixed',
                                               errors='coerce')
        user_df = user_df.dropna().reset_index(drop=True)
        return user_df

if __name__ == "__main__":
    df = DataCleaning.clean_user_data()
    print(df.info())