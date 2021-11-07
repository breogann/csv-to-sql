import os
import pandas as pd
from sqlalchemy import create_engine
import db_utils
from cleaning_df import dataframe_cleaning, export_csv
from workflow_utils import alerts, voiced_alerts, choosing_columns
from dotenv import load_dotenv

load_dotenv()


#0. CREATING DATABASE
def creatingDataBase(database):
    creating_the_database = f"""CREATE DATABASE IF NOT EXISTS {database};
    USE {database};"""
    engine.execute(creating_the_database)


#1. ESTABLISHING CONNECTION
def establishConnectionWithSQL(database, table):
    
    #Authentication
    my_sql_local = {
    'host':'localhost',
    'user':'root',
    'password':os.getenv("password"),
    'database':database,
    'table':table}

    connectionData=f"mysql+pymysql://root:{my_sql_local['password']}@localhost/{my_sql_local['database']}"
    
    global engine
    
    try:
        engine = create_engine(connectionData, pool_pre_ping=True)
        creatingDataBase(database)
        voiced_alerts("connection to database")
    except:
        voiced_alerts("failure connecting to db")



#2. CRUD Operations

#2.1. UPDATE
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
    else:
        columns_list = list(df.columns)

    #2. Second, this creates the table
    creating_the_table = f"""CREATE TABLE IF NOT EXISTS {table_name} (
    student_id VARCHAR (255),
    name VARCHAR (255),
    surname VARCHAR (255),
    country VARCHAR (255),
    update_date TIMESTAMP,
    created_date TIMESTAMP,
    PRIMARY KEY (student_id)
    );"""

    voiced_alerts("created table")
    
    engine.execute(creating_the_table)

    
    #3. Third, this populates the table
    
    counter = 0
    
    #3.1. This gets the row
    the_entire_row = []
    for index, rows in df.iterrows():
        for columna in columns_list:
            the_entire_row.append(rows[columna])

        #3.2. This inserts the row
        columns = str(tuple(columns_list[1:])).replace("'", "")
        insert_row_query = f"""
        INSERT IGNORE INTO {table_name} {columns} VALUES {tuple(the_entire_row[1:])};
        """
                
        results = engine.execute(insert_row_query)
        the_entire_row = []
        
        #3.3. Counting the modified rows
        affected_rows = results.rowcount
        if affected_rows >= 1:
            counter +=1
    
    return voiced_alerts("Inserted rows", counter)


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
        