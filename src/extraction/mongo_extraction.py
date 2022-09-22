import streamlit as st
import pandas as pd

from pymongo import MongoClient

def connectingMongoAtlas(database, collection):

    # Establish connection to the Cluster
    mongo_url = st.secrets["MONGO_CONNECTION_STRING"]
    client = MongoClient(mongo_url)

    # Specifies database & collection
    db = client[database]
    return db.get_collection(collection)

exams = connectingMongoAtlas("source_exams", "source_exams")

def queryMongoAtlas():

    # Query & projection
    filter_ = {}
    projection = {"_id":0, "student_id": 1, "cohort_id":1, "campus_id":1, "teacher_id":1, "score_value":1}

    # Exporting results into csv
    df_exams = pd.DataFrame(list(exams.find(filter_, projection)))
    df_exams.to_csv("output/mongo_atlas.csv", index=False)

    return df_exams


def refresh_csv ():
    exams = connectingMongoAtlas("source_exams", "source_exams")
    df = queryMongoAtlas()
    df = pd.read_csv("output/google_sheets.csv")
    return f"A csv file with {df.shape[0]} rows has been downloaded from MongAtlas"

refresh_csv()