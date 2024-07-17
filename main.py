import pandas as pd
import sqlite3
import os

data_directory = "data"
output_data = "output_data"

conn = sqlite3.connect("crypto_data.db") # connect to SQLite 3 Database

for i in os.listdir(data_directory):
    if i.endswith(" .csv") and i.startswith("2024"):
        id, date = i.split("-")[3:]  #extract Id and date from given data files

    data_new = pd.read_csv(os.path.join(data_directory,i))

    data_new.to_sql(
        f"exchange_{id}",
        conn,
        index = False,
        if_exists="replace",

    )

#now we need to commit the changes

conn.commit()
conn.close()
print("data has transferred into SQL Successfully")
