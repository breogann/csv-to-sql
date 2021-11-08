import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy_utils import create_database, database_exists
from src.cleaning_df import dataframe_cleaning, export_csv
from src.workflow_utils import alerts, voiced_alerts, choosing_columns
from dotenv import load_dotenv

load_dotenv()


#1. ESTABLISHING CONNECTION
def establishConnectionWithSQL(database, table):
    """SQLalchmey establishes the connection to the DB by using DB and table & creates the DB.
    :param database: The database to connect to.
    :param table: The table we'll be using.
    :return: None
    """

    #Authentication
    my_sql_local = {
    'user':'root',
    'password':os.getenv("password"),
    'host':'localhost',
    'port':3306,
    'database':{database}}

    #Getting list of DB
    global engine
    engine = create_engine('mysql://{0}:{1}@{2}:{3}'.format(my_sql_local['user'], my_sql_local['password'], my_sql_local['host'], my_sql_local['port']))
    existing_databases = engine.execute("SHOW DATABASES;")
    existing_databases = [d[0] for d in existing_databases]

    #Create if it doesn't exist
    if database not in existing_databases:
        engine.execute(f"""CREATE DATABASE IF NOT EXISTS {database} 
        CHARACTER SET latin1 COLLATE latin1_german1_ci;""")
    
    engine.execute(f"""USE {database};""")
    
#2. CRUD Operations

#2.1. UPDATE
def updating_value(table, column, new_value, old_value):
    """Updates a field in a table.
    :param table: The df to export.
    :param column: Column of the value.
    :param new_value: New value to be inputted.
    :param old_value: Old value to meet the condition.
    :return: None
    """

    query_update="""SET SQL_SAFE_UPDATES = 0;"""
    
    query=f"""
    UPDATE {table}
    SET {column}='{new_value}'
    WHERE {column}='{old_value}';
    """

    engine.execute(query)
    engine.execute(query_update)


def insert_df_into_db(df, table_name, columns_list=False):
    """Inserts a dataframe given into a default database. We need to provide the columns in the dataframe in the
    same order we want to insert them in the database.
    :param df: The dataframe we want to insert
    :param table_name: The table of the database we want to insert the data in
    :param columns_list: The columns of the table in the database we want to
insert the data in
    :return: The total number of rows inserted
"""
    if columns_list==True:
        columns_list = choosing_columns(df)

    elif columns_list==False:
        columns_list = list(df.columns)

    columns_list = list(df.columns)

    
    #2. Second, this creates the table
    creating_the_table = f"""CREATE TABLE IF NOT EXISTS {table_name} (
    student_id VARCHAR (255) NOT NULL,
    name VARCHAR (255),
    surname VARCHAR (255),
    country VARCHAR (255),
    update_date TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    created_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (student_id)
    );
    """

    update = "SET SQL_SAFE_UPDATES = 0;"


    voiced_alerts("created table")
    
    engine.execute(creating_the_table)
    engine.execute(update)

    
    #3. Third, this populates the table
    
    counter = 0
    
    #3.1. This gets the row
    the_entire_row = []
    for index, rows in df.iterrows():
        for columna in columns_list:
            the_entire_row.append(rows[columna])
        print(the_entire_row)
            

        #3.2. Query to insert the row
        columns = str(tuple(columns_list[0:])).replace("'", "")
        insert_row_query = f"""
        INSERT IGNORE INTO {table_name} {columns} VALUES {tuple(the_entire_row[0:])};
        """

        #3.3. Query to update the row
        update_existing_values_hardcoded=f"""UPDATE {table_name} SET name = '{the_entire_row[1]}', Surname = '{the_entire_row[2]}', country = '{the_entire_row[3]}' WHERE student_id = '{the_entire_row[0]}';"""
        #update_existing_values=f"""INSERT INTO {table_name} {columns} VALUES {tuple(the_entire_row[0:])} ON DUPLICATE KEY UPDATE student_id ='{the_entire_row[0]}';"""
        
        #3.4. Running the queries
        results_insert = engine.execute(insert_row_query)
        results_update = engine.execute(update_existing_values_hardcoded)
        
        the_entire_row = [] 
        
        #3.5. Counting the updated rows
        inserted_rows = results_insert.rowcount

        if inserted_rows >= 1:
            counter +=1
    
    updating_value(table_name, "name", "biyonsé", "BEYONCE") #Example to check that update and creation dates work
    voiced_alerts("Inserted and modified rows", counter)
    return counter


# 2.2. READ
def get_df_from_query(table_name, table_name_cols=False):
    """Launches a query in the default database and converts it into a
dataframe. It takes the names of the columns returned and puts it as names in the dataframe if table_name_cols=True
    :param query: The sql query we want to send
    :param table_name_cols: If True it returns that df with the same names cols as in the query
    :return: The dataframe generated
    """

    query = f"SELECT * FROM {table_name}"
    
    if table_name_cols==False:
        sql_to_pd = pd.read_sql_query(query, engine)
        return export_csv(sql_to_pd, "output/sql_table.csv", open=True)

    else:
        #Obtain the names of the columns of the SQL table
        sql_columns = f"""SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}';"""
        columns = list(engine.execute(sql_columns))
        new_columns = [title[0] for title in columns]
        print(new_columns)
        
        #Overwrite the name of the columns
        df = pd.read_sql_query(query, engine)
        df = df[new_columns]

        return export_csv(df, "output/sql_table.csv", open=True)