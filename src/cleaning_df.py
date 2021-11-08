import os
import pandas as pd
import numpy as np
from src.workflow_utils import alerts, voiced_alerts

def export_csv(df, path, open=False):
    """Exports a df into a given location with NaN values instead of empty and is verbose.
    :param df: The df to export.
    :param path: The path where the df will be exported.
    :param open: If True, the system will open the file.
    :return: None
    """

    df.to_csv(path, index=False, na_rep=np.nan)
    print(f"Dataframe exported @ {path}")
    print(df)
    voiced_alerts("df exported")
    if open==True:
        os.system(f"open {path}")

def dataframe_cleaning (path):
    """Does basic formatting of the text and exports the document into the output directory.
    :param path: The path where to read the csv from.
    :return: The processed df.
    """

    df = pd.read_csv(path)
    df = df.applymap(lambda x: x.replace("'", ""))

    export_csv(df, "output/cleaned_input.csv")
    df = pd.read_csv("output/cleaned_input.csv")
    voiced_alerts("reading and cleaning df")
    return df