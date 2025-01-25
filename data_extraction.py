import database_utils as dbut
from sqlalchemy import inspect
class DataExtractor:
    def list_rds_tables():
        engine = dbut.DatabaseConnector.init_db_engine()
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        return tables

tables = DataExtractor.list_rds_tables()

for t in tables:
    print (t)