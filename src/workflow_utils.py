
from logging import raiseExceptions
import os

alerts = {"reading and cleaning df":["Leyendo datafreim...", "Limpiando datafreim..."],
"failure connecting to db":["Base de datos conectada"],
"connecting the database":["Conexión establecida 🚀"],
"Inserted and modified rows":[f"Se han modificado # filas"],
"created table":["Se ha creado la tabla"],
"df exported":["Datafreim exportado"],
"choosing columns":["Elija las columnas"]}

def voiced_alerts(key, *counter):
    """Gives written and oral feedback on the tasks, accordign to the alerts dictionary.
    :param key: The value from which key to give feedback.
    :param counter: optional argument. We use this to give feedback on the quantity of things processed.
    :return: None
    """

    for i in alerts[key]:
        if "#" in i:
            i = i.replace("#", str(counter[0]))
        print(i)
        os.system("say -v Monica " + i)


def choosing_columns(df):
    """This will give a dictionary of the columns of a df so that the user can input which ones they want to use. Input should be by separating numbers by commas.
    :param df: The value from which key to give feedback.
    :return: a list of columns.
    """

    col_dict = {

    }
    
    for index, element in enumerate(df.columns):
        col_dict[index]=element
    
    voiced_alerts("choosing columns")
    columns=input(f"Here's the list of columns: {col_dict}.\n Choose the ones you want to keep by inputting the numbers separated by a comma, as in: \n Ex: 0,1,2,3\n")
    chosen_columns = columns.replace(" ", "").split(",")

    for i in chosen_columns:
        if type(int(i)) != int:
            raise ValueError("Only input numbers and commas")
            choosing_columns(df)
        elif int(i) > len(df.columns):
            raise ValueError("The column doesn't exist")
            choosing_columns(df)

    new_list = []
    for i in chosen_columns:
        new_list.append(col_dict[int(i)])
    
    print("You chose the following columns: ", new_list)

    return new_list

