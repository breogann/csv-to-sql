column_types = {
        "int": ["SCORE_VALUE"],
        "str":["SOURCE_STUDENT_ID", "NAME", "SURNAME", 
        "COUNTRY", "COHORT_ID", "CAMPUS_ID", "TEACHER_ID"]
}

def checkTargetType (column, cols = column_types):
    """Given a specific column, check the target key. That is, knowing the type of the
    target table, does the type of it correspond to the original one?
    :param column: string, specific column within the dataframe.
    :param cols: default, a dictionary with keys as type of data and list of column names as values.
    :returns: the target type of a column
    """
    for key, value in cols.items():
        if column in value:
             if key == "str":
                return str
             elif key == "int":
                return int
        
def checkElementsOfColumn (df, column):
    """It raises an error if it finds a cell with a different data type than it should have.
    :param df: pandas dataframe.
    :param column: the column to check.
    :returns: none. 
    """
    for element in list(df[column]):
        if type(element) == checkTargetType(column):
            print("Correct type of column")
        elif type(element) != checkTargetType(column):
            raise ValueError(f"""Incorrect data type on: column {column}, 
            position: {list(df[column]).index(element)}, 
            value: {element} should be type {checkTargetType(column)}
            but is type: {type(element)}""")

def allColumns (df):
    """Iterates over every column of a dataframe to check for data types.
    :param df: pandas dataframe.
    :returns: none. 
    """
    for i in list(df.columns):
        checkElementsOfColumn (df, i)