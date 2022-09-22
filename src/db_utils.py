import pandas as pd
import os
import streamlit as st
import snowflake.connector
from snowflake.connector.pandas_tools import pd_writer
from sqlalchemy.engine.url import URL

from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL

# 1. ESTABLISH CONNECTION
def establishConnectionWithSnowflake():
    return snowflake.connector.connect(
        user = st.secrets["SNOWFLAKE_USER"],
        password = st.secrets["SNOWFLAKE_PASSWORD"],
        account = st.secrets["SNOWFLAKE_ACCOUNT"]
    )
    
connection = establishConnectionWithSnowflake()
cursor = connection.cursor()

# 2. CREATES TABLES
last_created_table = """SELECT table_schema,
                           table_name,
                           created,
                           last_altered
                                FROM information_schema.tables
                                WHERE created > DATEADD(DAY, -30, CURRENT_TIMESTAMP)
                                      AND table_type = 'BASE TABLE'
                                ORDER BY created desc
                                LIMIT 1;"""

def createsTables (create_table_query, database = "CASE_STUDY"):  
    try: 
        connection = establishConnectionWithSnowflake()
        cursor = connection.cursor()
        cursor.execute(f"USE {database};")
        cursor.execute(create_table_query)
        cursor.execute("USE WAREHOUSE WAREHOUSE_1;")
        
        # Get the name of the table
        df = pd.read_sql(last_created_table, con=connection)
        table_name = df.iloc[0]["TABLE_NAME"]
        
        # Get the table
        query = f"SELECT * FROM {table_name};"
        df_2 = pd.read_sql(query, con=connection)
        
        print(df_2)

        one_row = cursor.fetchone()
        print(one_row[0])

    finally:
        cursor.close()
    connection.close()
    print("Table has been created")

create_warehouse_students = """
CREATE TABLE IF NOT EXISTS warehouse_students
(
    id                int IDENTITY(1,1) PRIMARY KEY,
    source_student_id varchar(255),
    name              varchar(255),
    surname           varchar(255),
    country           varchar(255),
    update_date       timestamp,
    created_date    timestamp
);
"""


create_warehouse_exams = """
CREATE TABLE IF NOT EXISTS warehouse_exams 
(
    id                int IDENTITY(1,1) PRIMARY KEY,
    source_student_id varchar(255),
    cohort_id         varchar(255),
    campus_id         varchar(255),
    teacher_id        varchar(255),
    update_date       timestamp,
    created_date    timestamp,
    score_value       int
);
"""


# 3. READ FILES TO INSERT
df_students = pd.read_csv("output/google_sheets.csv")
df_exams = pd.read_csv("output/mongo_atlas.csv")
df_students.columns = ["SOURCE_STUDENT_ID", "NAME", "SURNAME", "COUNTRY"]
df_exams.columns = ["SOURCE_STUDENT_ID", "COHORT_ID", "CAMPUS_ID", "TEACHER_ID", "SCORE_VALUE"]



# 4. SNOWFLAKE AUTHENTICATION & CONNECTION
def engineSnowflake():
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

engine = engineSnowflake()

def selectFromTable (table):
    query = engine.execute(f"SELECT * FROM {table}")
    return pd.read_sql(query, con=connection)


create_temporary_students_table = """MERGE INTO warehouse_students
    USING temporary_warehouse_students
    ON warehouse_students.source_student_id = temporary_warehouse_students.source_student_id
    WHEN matched THEN
        UPDATE SET warehouse_students.update_date = CURRENT_TIMESTAMP
    WHEN NOT matched THEN
        INSERT (source_student_id, name, surname, country, update_date, created_date)
            VALUES (temporary_warehouse_students.source_student_id,
            temporary_warehouse_students.name,
            temporary_warehouse_students.surname,
            temporary_warehouse_students.country,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
            ); """


create_temporary_exams_table = """MERGE INTO warehouse_exams
USING temporary_warehouse_exams
ON warehouse_exams.source_student_id = temporary_warehouse_exams.source_student_id
    WHEN matched THEN
        UPDATE SET warehouse_exams.update_date = CURRENT_TIMESTAMP
    WHEN NOT matched THEN
    INSERT (source_student_id, cohort_id, campus_id, teacher_id, update_date, created_date, score_value)
        VALUES (temporary_warehouse_exams.source_student_id,
        temporary_warehouse_exams.cohort_id,
        temporary_warehouse_exams.campus_id,
        temporary_warehouse_exams.teacher_id,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP,
        temporary_warehouse_exams.score_value
        ); """


# 5. INSERT: insert skipping duplicates using temp table
def insertIntoSnowflake(table, df, query):
    connection = establishConnectionWithSnowflake()
    cursor = connection.cursor()
    cursor.execute("USE WAREHOUSE WAREHOUSE_1;")
    cursor.execute("USE CASE_STUDY;")
    
    # 5.1. Load the new info into a temporary table in Snowflake
    df.to_sql(name=f'temporary_{table}',
                    con = engine,
                    if_exists = 'replace', 
                    method = pd_writer,
                    index=False)


    print("TEMPORARY TABLE LOADED")

    connection = establishConnectionWithSnowflake()
    cursor = connection.cursor()
    cursor.execute("USE WAREHOUSE WAREHOUSE_1;")
    cursor.execute("USE CASE_STUDY;")

   # 5.2. UPDATING / MERGING THE INFO
   # Create if it's not created
    if table == "warehouse_students":
        createsTables (create_warehouse_students)
    elif table == "warehouse_exams":
        createsTables (create_warehouse_exams)

    cursor.execute(query)

    print("INFO IS MERGED")
    
    # 5.3. Drop the temporary table
    delete_temporary_table = f'DROP TABLE temporary_{table}'
    cursor.execute(delete_temporary_table)

def selectEverythingSnowflake(table):
    connection = establishConnectionWithSnowflake()
    cursor = connection.cursor()
    cursor.execute("USE WAREHOUSE WAREHOUSE_1;")
    cursor.execute("USE CASE_STUDY;")
    query_2 = f'SELECT * FROM {table};'
    return pd.read_sql(query_2, con=connection)

def createView ():
    connection = establishConnectionWithSnowflake()
    cursor = connection.cursor()
    cursor.execute("USE WAREHOUSE WAREHOUSE_1;")
    cursor.execute("USE CASE_STUDY;")

    cursor.execute("""
            CREATE VIEW IF NOT EXISTS warehouse_progress AS 
            SELECT students.source_student_id, name, surname, country, score_value
                FROM warehouse_students as students
                    JOIN warehouse_exams as exams 
                    ON students.source_student_id=exams.source_student_id;
                    """)

    query = 'SELECT * FROM warehouse_progress;'
    return pd.read_sql(query, con=connection)

