import argparse
import pandas as pd
import os
from src.db_utils import establishConnectionWithSQL, insert_df_into_db, get_df_from_query
from src.cleaning_df import dataframe_cleaning


def parse_args():
    parser = argparse.ArgumentParser(description="Path to the file")
    
    #The arguments:
    parser.add_argument("--path", required=True, help="Absolute path, as this is not deployed yet")
    parser.add_argument("--database", required=True, help="SQL DB to connect to")
    parser.add_argument("--table", required=True, help="The table within the SQL database")
    parser.add_argument("--columns", required=False, help="Columns are by default the existing ones in the CSV. Choose True if you want to select")
    parser.add_argument("--sqlcolumns", required=False, help="Columns to retrieve from SQL. False is every column. True is the chosen ones")


    return parser.parse_args()

if __name__ == '__main__':
    args = parse_args()
    path = args.path
    database = args.database
    table = args.table
    columns_list = args.columns
    table_name_cols = args.sqlcolumns
    
    df = dataframe_cleaning(path)
    establishConnectionWithSQL(database, table)
    insert_df_into_db(df, table, columns_list)
    get_df_from_query(table, table_name_cols=table_name_cols)