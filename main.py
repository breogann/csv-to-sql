from multiprocessing.reduction import DupFd
import streamlit as st
from PIL import Image
from src.extraction import googlesheets_extraction, mongo_extraction
import src.db_utils as sfk
from src.db_utils import df_students, df_exams
from src.db_utils import create_temporary_students_table, create_temporary_exams_table
import pandas as pd
import plotly.express as px

st.set_page_config(
     page_title="Data Ironhack",
     page_icon="📊",
     layout="wide",
     initial_sidebar_state="collapsed",
     menu_items={
         'Get Help': 'https://github.com/breogann/csv-to-sql/tree/case-study',
         #'Report a bug': "https://www.extremelycoolapp.com/bug",
         'About': "# This is a header. This is an *extremely* cool app!"
     }
 )

col1, col2, col3 = st.columns([1,6,1])

with col1:
    st.write("")

with col2:
    st.markdown("<h1 style='text-align: center; color: grey;'>Data Ironhack</h1>", unsafe_allow_html=True)
    cover = Image.open("imgs/cover.jpeg")
    st.image(cover, use_column_width=True)

    st.markdown("""
    This application
    1. Extracts students information from **tables**: GoogleSheets
    2. Extracts exams information from a **non-relational DB**: MongoAtlas
    3. Loads it into a **relational database in a warehouse**: Snowflake
    4. Creates a View for the progress (if not exists)
    5. Lets you visualize students' progress by campus, teacher and course.
    """)


    st.markdown("""
    How you can explore it
    - You're right now on the main page. Here's a step-by-step demonstration.
    - To run the whole process at once, go to pipeline page (menu on the left)
    - Details for code below
    """)



    # 1. FETCH STUDENTS FROM GOOGLE SHEETS
    st.markdown("<h2 style='text-align: center; color: grey;'>1. Fetch the information</h2>", unsafe_allow_html=True)
    image_gs = Image.open('imgs/googlesheets.png')
    st.image(image_gs, caption='Google sheet table: students')
    students = st.checkbox('Fetch students 👩‍🎓')
    if students:
        with st.spinner('Downloading google sheet'):
            st.markdown("""Here's the link to the [sheet](https://docs.google.com/spreadsheets/d/1WmAxVLwRVMinB-TzShm4iHoVFjXs0RNwWeuY7mAM6YE/edit#gid=0).
             You can make changes to the public sheet and check the box again to get updated information""")
            st.write(googlesheets_extraction.refresh_csv())
            st.dataframe(data = pd.read_csv("output/google_sheets.csv"))
            st.success('Students fetched!')

    # 2. FETCH EXAMS FROM MONGO ATLAS
    st.markdown("<h2 style='text-align: center; color: grey;'>2. Fetch exams 📝</h2>", unsafe_allow_html=True)
    image_mongo = Image.open('imgs/mongoatlas.png')
    st.image(image_mongo, caption='MongoAtlas database: exams')

    exams = st.checkbox('Fetch exams 📝')
    if exams:
        with st.spinner('Querying MongoAtlas'):
            st.write(mongo_extraction.refresh_csv())
            st.dataframe(data = pd.read_csv("output/mongo_atlas.csv"))
            st.success('Exams fetched!')

    # 3. LOAD INTO SNOWFLAKE
    st.markdown("<h2 style='text-align: center; color: grey;'>3. Load tables into snowflake ❄️</h2>", unsafe_allow_html=True)
    image_snowflake = Image.open('imgs/snowflake.png')
    st.image(image_snowflake, caption='Snowflake warehouse: students table')

    ## 3.1. Students & exams
    update = st.checkbox('Update students & exams tables 👩‍🎓')
    if update:
        with st.spinner('Merging updates'):
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
            st.table(df_students)

            st.markdown(hide_table_row_index, unsafe_allow_html=True)
            df_exams = sfk.selectEverythingSnowflake("warehouse_exams")
            st.table(df_exams)
            st.success('New students & exams loaded into Snowflake!')


    # 4. READ MERGED TABLE
    st.header("4. Progress table")
    check_view = st.checkbox('View progress table')
    if check_view:
        with st.spinner('Viewing view'):
            hide_table_row_index = """
            <style>
            thead tr th:first-child {display:none}
            tbody th {display:none}
            </style>
            """
            st.markdown(hide_table_row_index, unsafe_allow_html=True)
            sfk.createView()
            df_progress = sfk.selectEverythingSnowflake("warehouse_progress")
            st.table(df_progress)
            st.success("You're now looking at the score summary")

    # 5. Visualization by: 
    st.header("5. Quick visualization")
    option = st.selectbox(
    'What do you want to group it by?',
    ('COHORT_ID', 'CAMPUS_ID', 'TEACHER_ID'))
    st.write('You selected:', option)

    fig = px.bar(df_exams, x=option, y='SCORE_VALUE', barmode = 'group',text_auto = True)
    st.plotly_chart(fig)

    st.markdown("""
    To check the code
    - Here's the repository on [github](https://github.com/breogann/csv-to-sql/tree/case-study)
    - You can see a different approach, MySQL based [on here too](https://github.com/breogann/csv-to-sql/tree/option1)
            """)

with col3:
    st.write("")