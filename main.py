#  ------------------------------------------------- ###
#  ------------------------------------------------- ###
#  ### Developed by Anandavel Arasan. ###
#  _________________________________________________ ###
#  _________________________________________________ ###

import pyodbc
import configparser
from jinjasql import JinjaSql
from six import string_types
from copy import deepcopy
import readconfig
import os
from datetime import datetime

# Connect SQL server database

config = configparser.ConfigParser()
ini_path = os.path.join(os.getcwd(), 'config_file.ini')
config.read(ini_path)
server = config.get('Database_poc', 'server')
database = config.get("Database_poc", "database")
user = config.get("Database_poc", "user")
tds_version = config.get("Database_poc", "tds_version")
pwd = config.get("Database_poc", "password")
port = config.get("Database_poc", "port")
driver = config.get("Database_poc", "driver")
mssql_conn = pyodbc.connect(server=server, database=database, user=user, tds_version=tds_version, password=pwd, port=port)

PROJECT = config.get("Database_poc", "Project_name")
now = datetime.now()
params = {
 'transaction_date': now.strftime("%d/%m/%Y %H:%M:%S"),
}
# Add single quotes for input variable


def quote_sql_string(value):
    '''
    If `value` is a string type, escapes single quotes in the string
    and returns the string enclosed in single quotes.
    '''
    if isinstance(value, string_types):
        new_value = str(value)
        new_value = new_value.replace("'", "''")
        return "'{}'".format(new_value)
    return value

# Add single quotes for input variable

def get_sql_from_template(query, bind_params):
    if not bind_params:
        return query
    params = deepcopy(bind_params)
    for key, val in params.items():
        params[key] = quote_sql_string(val)
    return query % params


# open cursor and Select all test case from QA_TEST table


cursor = mssql_conn.cursor()
cursor.execute("SELECT * FROM dbo.qa_tests")
# cursor.execute("select * from qa_tests")
records = cursor.fetchall()
for row in records:
    test_case_id = row[0]
    print(test_case_id)
    sql_query = row[4]
    exp_result_match = int(row[5])
    # Dynamic Sql query from table input
j = JinjaSql(param_style='pyformat')
query, bind_params = j.prepare_query(sql_query, params)
sql_final = get_sql_from_template(query, bind_params)

cursor.execute(sql_final)
for output_qa_test in list(cursor):
    exp_query_match = int(output_qa_test[0])
    if exp_query_match == exp_result_match:
        # Audit table entry based on test case success and failure
        cursor.execute("""INSERT INTO AUDIT_TABLE (TEST_CASE_ID,PROJECT,EXECUTION_RESULT,EXECUTION_TIME)VALUES (?, ?, ?,?) """, (test_case_id, PROJECT,'SUCCESSFUL',datetime.today().strftime('%Y-%m-%d')))
        #            print('Test case execution completed successfully')
    else:
        cursor.execute("""INSERT INTO AUDIT_TABLE (TEST_CASE_ID,PROJECT,EXECUTION_RESULT,EXECUTION_TIME)VALUES (?, ?, ?,?) """, (test_case_id, PROJECT,'FAILED',datetime.today().strftime('%Y-%m-%d')))
        #            print('Test case execution failed')
    connection.commit()



