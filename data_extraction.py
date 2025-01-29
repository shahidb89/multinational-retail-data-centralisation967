import database_utils as dbut
import pandas as pd
from sqlalchemy import inspect
from tabula import read_pdf
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

    def retrieve_pdf_data(link):
        dfs = read_pdf(link, pages="all", multiple_tables=True)
        combined_df = pd.concat(dfs, ignore_index=True)
        return combined_df


user_df = DataExtractor.read_rds_table("legacy_users")



# tables = DataExtractor.list_rds_tables()

# for t in tables:
#     print (t)

if __name__ == "__main__":
    link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
    card_table = DataExtractor.retrieve_pdf_data(link)
    print(card_table)