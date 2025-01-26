import database_utils as dbut
import pandas as pd
from sqlalchemy import inspect
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


user_df = DataExtractor.read_rds_table("legacy_users")

print (user_df.head())
# tables = DataExtractor.list_rds_tables()

# for t in tables:
#     print (t)

# if __name__ == "__main__":

#     user_table = DataExtractor.read_rds_table("orders_table")
#     print(user_table.head(5))