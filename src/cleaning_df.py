import os
import pandas as pd
from src.workflow_utils import alerts, voiced_alerts

def export_csv(df, path, open=False):
    df.to_csv(path, index=False)
    print(f"Dataframe exported @ {path}")
    print(df)
    voiced_alerts("df exported")
    if open==True:
        os.system(f"open {path}")

def dataframe_cleaning (path):
    df = pd.read_csv(path)
    df = df.applymap(lambda x: x.upper())

    export_csv(df, "output/cleaned_input.csv")
    df = pd.read_csv("output/cleaned_input.csv")
    voiced_alerts("reading and cleaning df")
    return df





    
    