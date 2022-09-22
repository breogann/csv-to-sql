import streamlit as st
import src.db_utils as sfk
import pandas as pd
import plotly.express as px
import base64

from PIL import Image

from src.extraction import googlesheets_extraction, mongo_extraction
from src.db_utils import df_students, df_exams

from src.transformation import campuses
from src.snowflake_queries.create import create_temporary_exams_table, create_temporary_students_table
from src.snowflake_queries.update import create_temporary_students_table, create_temporary_exams_table

# 0. LAYOUT
col1, col2, col3 = st.columns([1,6,1])
with col1:
    st.write("")

with col2:
    st.markdown("<h1 style='text-align: center; color: grey;'>everyday</h1>", unsafe_allow_html=True)
    if st.button("Fetch, insert & select"):
        # 1. FETCH STUDENTS FROM GOOGLE SHEETS
        with st.spinner('Downloading google sheet'):
            st.write(googlesheets_extraction.refresh_csv())
            st.dataframe(data = pd.read_csv("output/google_sheets.csv"))
            st.success('Students fetched!')

        # 2. FETCH EXAMS FROM MONGO ATLAS
        with st.spinner('Querying MongoAtlas'):
            st.write(mongo_extraction.refresh_csv())
            st.dataframe(data = pd.read_csv("output/mongo_atlas.csv"))
            st.success('Exams fetched!')

        # 3. LOAD INTO SNOWFLAKE
        with st.spinner('Uploading and retrieving updates from snowflake'):

            st.markdown("![git merge](https://media.giphy.com/media/cFkiFMDg3iFoI/giphy.gif)")
            
            # Merge
            sfk.insertIntoSnowflake("warehouse_students", df_students, create_temporary_students_table)
            sfk.insertIntoSnowflake("warehouse_exams", df_exams, create_temporary_exams_table)

            # Read
            hide_table_row_index = """
            <style>
            thead tr th:first-child {display:none}
            tbody th {display:none}
            </style>
            """
            st.markdown(hide_table_row_index, unsafe_allow_html=True)
            df_students = sfk.selectEverythingSnowflake("warehouse_students")
            st.subheader("Table: warehouse_students")
            st.table(df_students)

            st.markdown(hide_table_row_index, unsafe_allow_html=True)
            df_exams = sfk.selectEverythingSnowflake("warehouse_exams")
            st.subheader("Table: warehouse_exams")
            st.table(df_exams)
            st.success('New students & exams loaded into Snowflake!')


        # 4. READ MERGED TABLE
        st.header("4. Progress table")
        with st.spinner('Viewing view'):
            hide_table_row_index = """
            <style>
            thead tr th:first-child {display:none}
            tbody th {display:none}
            </style>
            """
            st.markdown(hide_table_row_index, unsafe_allow_html=True)
            #sfk.createView()
            df_progress = sfk.selectEverythingSnowflake("warehouse_progress")
            st.table(df_progress)
            st.success("You're now looking at the score summary")

        # 5. Visualization by: 
        st.header("5. Quick visualization")
        df_exams.replace({"CAMPUS_ID": campuses}, inplace=True)
        fig = px.bar(df_exams, x="CAMPUS_ID", y='SCORE_VALUE', barmode = 'group',text_auto = True)
        st.plotly_chart(fig)
        
   
with col3:
    st.write("")