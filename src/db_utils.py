import pandas as pd
import streamlit as st
import snowflake.connector

from snowflake.connector.pandas_tools import pd_writer
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL

from src.snowflake_queries.create import create_warehouse_exams, create_warehouse_students, view
from src.snowflake_queries.read import last_created_table


# 1. ESTABLISH CONNECTION
def establishConnectionWithSnowflake():
    """Creates a connector for Snowflake for further creating a cursor.
    :return: a connector.
    """
    return snowflake.connector.connect(
        user = st.secrets["SNOWFLAKE_USER"],
        password = st.secrets["SNOWFLAKE_PASSWORD"],
        account = st.secrets["SNOWFLAKE_ACCOUNT"]
    )

def connect ():
    """Establishes a connection with Snowflake for further creating a cursor.
    :return: connection and cursor.
    """  
    connection = establishConnectionWithSnowflake()
    cursor = connection.cursor()
    cursor.execute("USE WAREHOUSE WAREHOUSE_1;")
    cursor.execute("USE CASE_STUDY;")
    return connection, cursor

# 2. CREATES TABLES
def createsTables (create_table_query, database = "CASE_STUDY"):
    """Establishes a connection with Snowflake for further creating a cursor.
    :param alcreate_table_query: string, query to create the table.
    :param database: string, default. the name of the database to which connect to. 
    :return: none.
    """  
    try: 

        # Connect  and create table
        connection, cursor = connect()
        cursor.execute(create_table_query)

        # Get the name of the table
        df = pd.read_sql(last_created_table, con=connection)
        table_name = df.iloc[0]["TABLE_NAME"]
        
        # Get the table
        query = f"SELECT * FROM {table_name};"
        df_2 = pd.read_sql(query, con=connection)
        
        one_row = cursor.fetchone()

    finally:
        cursor.close()
    connection.close()
    print("Table has been created")


# 3. READ FILES TO INSERT
def changeColumns(path, dict_):
    """Establishes a connection with Snowflake for further creating an engine.
    :param path: string, path to the df
    :param dict_: dictionary with keys as the old column names and values as the new one.
    :return: none.
    """
    df = pd.read_csv(path)
    df.rename(columns = dict_, inplace=True)
    return df

students_columns = {"student_id" : "SOURCE_STUDENT_ID",
        "name": "NAME",
        "Surname": "SURNAME",
        "country":"COUNTRY"}
 
exams_columns = {"student_id" : "SOURCE_STUDENT_ID",
        "cohort_id": "COHORT_ID",
        "campus_id": "CAMPUS_ID",
        "teacher_id":"TEACHER_ID",
        "score_value":"SCORE_VALUE"}


df_students = changeColumns("output/google_sheets.csv", students_columns)
df_exams = changeColumns("output/mongo_atlas.csv", exams_columns)



# 4. SNOWFLAKE AUTHENTICATION & CONNECTION FOR ENGINE
def engineSnowflake():
    """Establishes a connection with Snowflake for further creating an engine.
    :return: none.
    """  
    return create_engine(URL(
        account = st.secrets['SNOWFLAKE_ACCOUNT'],
        user = st.secrets["SNOWFLAKE_USER"],
        password = st.secrets["SNOWFLAKE_PASSWORD"],
        database = "CASE_STUDY",
        schema = "PUBLIC",
        role='ACCOUNTADMIN',
        cache_column_metadata=True,
        warehouse="WAREHOUSE_1"
    ))



def selectFromTable (table):
    """Queries a whole table on snowflake.
    :param table: string. the name of the table to which connect to. 
    :return: a pandas dataframe with the queried table.
    """  
    # Connection & engine 
    connection, _ = connect()
    engine = engineSnowflake()

    # Querying the table
    query = engine.execute(f"SELECT * FROM {table}")
    return pd.read_sql(query, con=connection)


# 5. INSERT: insert skipping duplicates using temp table
def insertIntoSnowflake(table, df, query):
    """Inserts a whole table on snowflake.
    :param table: string. the name of the table to which insert to. 
    :param df: string. the df to be inserted.
    :param query: string. query for insertion
    :return: none.
    """  

    _, cursor = connect()
    engine = engineSnowflake()

    
    # 5.1. Load the new info into a temporary table in Snowflake
    df.to_sql(name=f'temporary_{table}',
                    con = engine,
                    if_exists = 'replace', 
                    method = pd_writer,
                    index=False)

    _, cursor = connect()

   # 5.2. UPDATING / MERGING THE INFO
   # Create if it's not created
    if table == "warehouse_students":
        createsTables (create_warehouse_students)
    elif table == "warehouse_exams":
        createsTables (create_warehouse_exams)

    cursor.execute(query)
    
    # 5.3. Drop the temporary table
    delete_temporary_table = f'DROP TABLE temporary_{table}'
    cursor.execute(delete_temporary_table)

def selectEverythingSnowflake(table):
    """Queries a whole table on snowflake.
    :param table: string. the name of the table to which select from.
    :return: a pandas dataframe with the queried info.
    """  

    connection, _ = connect()
    query_2 = f'SELECT * FROM {table};'
    return pd.read_sql(query_2, con=connection)


def createView ():
    """Creates a view for warehouse_progress if it doesn't exist.
    :return: a pandas dataframe with the queried view.
    """  
    connection, cursor = connect()
    cursor.execute(view)

    query = 'SELECT * FROM warehouse_progress;'
    return pd.read_sql(query, con=connection)