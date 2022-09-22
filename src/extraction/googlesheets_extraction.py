import streamlit as st
import pandas as pd

from gsheetsdb import connect

sheet_url = st.secrets["public_gsheets_url"]
conn = connect()

def run_query(query):
    rows = conn.execute(query, headers=1)
    rows = rows.fetchall()
    return rows

def refresh_csv ():
    rows = run_query(f'SELECT * FROM "{sheet_url}"')
    df = pd.DataFrame(rows)
    df.to_csv("output/google_sheets.csv", index=False)
    return f"A csv file with {df.shape[0]} rows has been downloaded from GoogleSheets"
