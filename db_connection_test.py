# -*- coding: utf-8 -*-
"""
Created on Fri Jul  3 09:17:19 2020

@author: FS547CH

The goal is to open and close a connection to the database to test whether the
machine is connected to the lia VPN.
"""
from time import time
t1 = time()
from misc_fxns import set_local_variables
import update_dependency_tracker_table as db
import pickle
import os
set_local_variables()

"Create a boolean, save as a python pickle file"
lia_connected = False
with open(os.environ.get('pickle_path'), 'wb') as f:
    pickle.dump(lia_connected, f)

print("Attempting to connect to the database...")
test_conn, test_cursor = db.open_connection()
print("Connection opened.\n\nClosing the connection to the database...")
test_cursor.close()
test_conn.close()
t2 = time()

print("\nConnection closed.\nSwitching on the lia VPN indicator.")
lia_connected = True
with open(os.environ.get('pickle_path'), 'wb') as f:
    pickle.dump(lia_connected, f)
    
print("Total time: " + str(round(t2-t1,3)) + " seconds" )

