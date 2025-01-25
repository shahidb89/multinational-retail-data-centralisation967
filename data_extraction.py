import database_utils as dbut
from sqlalchemy import inspect
class DataExtractor:
    def read_rds_table():
        engine = dbut.DatabaseConnector.init_db_engine()
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        return tables

tables = DataExtractor.read_rds_table()

for t in tables:
    print (t)