import streamlit as st
import pandas as pd

from gsheetsdb import connect

sheet_url = st.secrets["public_gsheets_url"]
conn = connect()

def run_query(query):
    """Queries a table on snowflake.
    :param query: string with SQL syntax.
    :return: the rows of the table.
    """
    rows = conn.execute(query, headers=1)
    rows = rows.fetchall()
    return rows

def refresh_csv ():
    """Fetches and updates the new GoogleSheet rows into a local csv file.
    :return: a string with the amount of rows updated.
    """
    rows = run_query(f'SELECT * FROM "{sheet_url}"')
    df = pd.DataFrame(rows)
    df.to_csv("output/google_sheets.csv", index=False)
    return f"A csv file with {df.shape[0]} rows has been downloaded from GoogleSheets"
