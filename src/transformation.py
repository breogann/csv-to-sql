import pandas as pd

def changeColumns(path, dict_):
    """Establishes a connection with Snowflake for further creating an engine.
    :param path: string, path to the df
    :param dict_: dictionary with keys as the old column names and values as the new one.
    :return: a dataframe with the columns renamed and .
    """
    df = pd.read_csv(path)
    df.rename(columns = dict_, inplace=True)
    columns_in_csv_in_dict = [i for i in df.columns if i in dict_.values()]
    return df[columns_in_csv_in_dict]

students_columns = {"student_id" : "SOURCE_STUDENT_ID",
        "name": "NAME",
        "Surname": "SURNAME",
        "country":"COUNTRY"}
 
exams_columns = {"student_id" : "SOURCE_STUDENT_ID",
        "cohort_id": "COHORT_ID",
        "campus_id": "CAMPUS_ID",
        "teacher_id":"TEACHER_ID",
        "score_value":"SCORE_VALUE"}

campuses = {
"5469924v5dw42":"Madrid",
"92346vlz0356":"Barcelona",
"54624v5dw420":"Mexico",
"54624v5dw421":"US",
"54624v5dw422":"Brazil",
"54624v5dw423":"Paris",
"54624v5dw424":"Amsterdam"
}