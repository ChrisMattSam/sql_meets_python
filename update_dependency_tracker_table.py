# 6/17/20
# Christopher Sampah

import pyodbc
import pandas as pd
import os
import numpy as np
from datetime import datetime
import pickle
from time import time
from misc_fxns import set_local_variables

"blahb lah bafdflasdkfjasldfasf"
def open_connection(admin = True):
    """ 
    Open a connection to the SQL database of interest
    """
    from misc_fxns import login_key
    
    u,p = 'admin_username', 'admin_password'
    server, database = login_key(admin_username_key = u, admin_password_key = p)
    u, p = os.environ.get(u), os.environ.get(p)
    if admin:
        conn = pyodbc.connect(('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+
                               server+';DATABASE='+database+';UID='+u+';pwd='+
                               p+';'))
    else:
        conn = pyodbc.connect(('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+
                               server+';DATABASE='+database+';UID='+u+
                               ';Authentication=ActiveDirectoryInteractive;'))
    cursor = conn.cursor()
    return conn, cursor

def alter_db(query, suppress_print = False):
    """
    Execute one query upon the database that doesnt involve extracting data
    """
    if not suppress_print:
        print("The query to be executed is: {}".format(query))
    
    conn, cursor = open_connection()
    cursor.execute(query) # execute the query
    conn.commit() # enact the query on the database
    # Close the connection so that others can modify the db as needed
    cursor.close()
    conn.close()

def check_existence(table_name):
    """
    Taking in the name of a table as a string formatted like <schema>.<table>,
    the function returns whether this table exists in the database or not
    """
    schema, table = table_name.split(".")
    t = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}'".format(schema, table)
    if extract_data(t).shape[0] == 0:
        return False
    else:
        return True

def extract_data(query):
    """
    Query the database for information from a table or view, and return that 
    information as a dataframe
    """
    conn, cursor = open_connection()
    sql_df = pd.read_sql(query, conn) # pull the data from the db
    # Close the connection so that others can modify the db as needed
    cursor.close()
    conn.close()
    return sql_df

def insert_data(from_df, into_table = 'z_cs.dependency_tracker_bkp',
                suppress_print = True):
    """
    Insert data from a pandas dataframe into a table within the database
    """
    query = insert_query(from_df, into_table)
    if not suppress_print:
        s1,s2 = query.split('VALUES')
        print("The query to be executed is: "+s1 + "VALUES" + s2.split(')')[0]+ ')...')
        
    alter_db(query, suppress_print = True)

def insert_query(from_df, into_table = 'z_cs.dependency_tracker_bkp'):
    """
    Create one insert statement comprised of values of a pandas dataframe
    """
    
    insert_string = ""
    for row in from_df.iterrows():
        """To make this smarter, could just search the column names of the df and
        specify number of columns instead of hard-coding"""
        row = row[1]
        s1 = "('" + str(row["DataSchema"]) + "','" + str(row["Data"]) + "','" + str(row["DataType"])
        s2 = "','" + str(row["TrackedItem"]) + "','" + str(row["Notes"]) + "','" + str(row["ViewCreated"])
        s3 = "','" + str(row["DbFlag"]) + "')"
        insert_string = insert_string + s1 + s2 + s3 + ","
    
    query = "INSERT INTO " + into_table + " VALUES " + insert_string[:-1]
    return query

def update_internal_dependencies(location_of_tracker, internal_query):
    """
    Read-in the current Excel Dependency Tracker file and drop all internal
    dependencies, query the internal dependencies directly from the database,
    then merge these two tables into one table and return it
    """
    i_df = extract_data(internal_query) # query the database for internal dependencies
    i_df.loc[i_df["Data"].str[-3:] == '_vw', ['DataType']] = "View" #specify DataType as 'view' where needed
    current_tracker = pd.read_excel(location_of_tracker) # upload the current dependency tracker Excel file
    # Make a backup copy just in case, also to log the script
    timestamp = str(datetime.now())[:-7].replace(":",".")
    current_tracker.to_csv(os.environ.get('backup_path') + timestamp +".csv", index = False)
    
    corrected_tracker = pd.concat([i_df, current_tracker.loc[current_tracker['DbFlag'] == 0]]) # merge the two tables
    corrected_tracker.replace(np.nan, 'NULL', inplace = True)
    return corrected_tracker

    
if __name__ == "__main__":
    
    t0 = time()
    set_local_variables()
    original_file = os.environ.get('tracker_path')
    current_tracker = os.environ.get('current_file')
    backup_tracker = os.environ.get('backup_file')
    
    internal_query = """
    SELECT DISTINCT [DataSchema] = referenced_schema_name,[DataType] = 'Table', [Data] = referenced_entity_name, 
    TrackedItem = CONCAT(SCHEMA_NAME(o.[schema_id]),'.' ,o.[name]), DbFlag = 1, Notes = 'NULL', ViewCreated = 'NULL'
    FROM sys.sql_expression_dependencies e
    INNER JOIN sys.objects o ON e.referencing_id = o.[object_id]
    WHERE referenced_schema_name NOT in ('dbo')
    ORDER BY [TrackedItem]"""
    
    
    with open(os.environ.get('pickle_path'), 'rb') as f:
        lia_connected = pickle.load(f)
        
    if lia_connected is False:
        print("Not connected to the LIA vpn. Cannot query the database")
    
    else:
        
        print("Creating the updated Dependency Tracker table to be read into the database...")
     
        df = update_internal_dependencies(original_file, internal_query)
        
        df.sort_values(by = ['TrackedItem',"DataSchema", "Data"], inplace = True)
        print("Done\n\nTransferring the current database Dependency Tracker into the backup table...")
        if check_existence(backup_tracker) is True: alter_db("TRUNCATE TABLE {}".format(backup_tracker))
        
        insert_data(from_df = extract_data("SELECT * FROM {}".format(current_tracker)),
                    into_table = backup_tracker, suppress_print = False)
                
        print("Done.\n\nDeleting the contents of the current Dependency Tracker table...")
        alter_db("TRUNCATE TABLE {}".format(current_tracker))
        print("Done.\n\nInserting updated values into the Dependency Tracker table in the database...")
        insert_data(df, current_tracker,True)
        print("Done")
        t1 = time()
        print("Total time: " + str(round(t1-t0,3)) + " seconds")
        
        
    
        