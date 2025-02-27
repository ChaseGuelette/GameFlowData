#script for retreiving the data stored on virtual server

#imports 
from sqlalchemy import text, create_engine
import pandas as pd

#function definitions:

#given a local and remote engine, and a table name
#download the remote table to the local database
def sync_individual_table(table_name, remote_engine, local_engine):
   df = pd.read_sql(f"SELECT * FROM {table_name}", remote_engine)
   
   # Check if table exists
   with local_engine.connect() as conn:
       exists = conn.dialect.has_table(conn, table_name)
       
   if exists:
       with local_engine.connect() as conn:
           conn.execute(text(f"DELETE FROM {table_name}"))
       df.to_sql(table_name, local_engine, if_exists='append', index=False)
       print("success!")
   else:
       df.to_sql(table_name, local_engine, if_exists='fail', index=False)
       print("success!")

#function that uses SQL querys to get list
#of all tables in the remote database
def get_remote_tables(remote_engine):
    query = """
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public'
    """
    return pd.read_sql(query, remote_engine)['table_name'].tolist()


#function that uses get remote tabels and sync_individal_table to sync all tables
#that are in the remote database
def sync_all_tables(remote_engine, local_engine):
    tables = get_remote_tables(remote_engine)
    for table in tables:
        sync_individual_table(table, remote_engine, local_engine)
        print(f"{table} successfully synced!")



#create local and remote engines
remote_engine = create_engine('postgresql://nba_user:Black-apple32@157.245.122.204:5432/nba_data')
local_engine = create_engine('postgresql://postgres:Black-apple32@localhost:5433/TeamData')

#call functions to retreive data
tables = get_remote_tables(remote_engine)
sync_all_tables(remote_engine, local_engine)

print("Data Sync Complete")



