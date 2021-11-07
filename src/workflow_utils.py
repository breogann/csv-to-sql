
import os

alerts = {"reading and cleaning df":["Leyendo datafreim...", "Limpiando datafreim..."],
"failure connecting to db":["Base de datos conectada"],
"connecting the database":["Conexión establecida 🚀"],
"Inserted rows":[f"Se han insertado # filas"],
"created table":["Se ha creado la tabla"],
"df exported":["Datafreim exportado"]}

def voiced_alerts(key, *counter):
    for i in alerts[key]:
        if "#" in i:
            i = i.replace("#", str(counter[0]))
        print(i)
        return os.system("say -v Monica " + i)


def choosing_columns(df):
    
    col_dict = {

    }
    
    for index, element in enumerate(df.columns):
        col_dict[index]=element
    
    columns=input(f"Here's the list of columns: {col_dict}.\n Choose the ones you want to keep by inputting the numbers separated by a comma, as in: \n Ex: 0,3\n")
    chosen_columns = columns.split(",")

    new_list = []
    for i in chosen_columns:
        new_list.append(col_dict[int(i)])
    
    print("You chose the following columns: ", new_list)

    return new_list

