import pandas as pd
import sqlite3
import os
from  datetime import  datetime
data_directory = "data"
output_data = "output_data"

conn = sqlite3.connect("crypto_data.db") # connect to SQLite 3 Database
cur = conn.cursor()

for i in os.listdir(data_directory):
    if i.endswith(" .csv") and i.startswith("2024"):
        id, date = i.split("-")[3:]  #extract Id and date from given data files

    data_new = pd.read_csv(os.path.join(data_directory,i))
    data_new["date"] = pd.to_datetime(data_new['date']) #make sure that date column is in right format

    if "id" not in data_new.columns:
        data_new["id"] = range(1,len(data_new)+1)

    table_name = "table" + os.path.splitext(os.path.basename(i))[0]

    data_new.to_sql(
        table_name,
        conn,
        index = False,
        if_exists="replace",

    )

def create_joinable_key(timestamp):
    return timestamp.replace(":","").replace("-","").replace("_","")

#we need list of the excel files from the folder

csv_files = [f for f in os.listdir(data_directory) if f.endswith(".csv")]

tables_names = list(set(f.split("_")[-1].replace(".csv","") for f in csv_files))

for table in tables_names:
    try:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN joinable_key TEXT")
    except sqlite3.OperationalError as e:
        print(f'Skipping {table}:{e}')

for table in tables_names:
    try:
        cur.execute(f"SELECT rowid,date FROM {table}")
        rows = cur.fetchall()
        for row in rows:
            rowid,date = row
            joinable_key = create_joinable_key(date)
            cur.execute(f"UPDATE {table} SET joinable_key = ? WHERE rowid = ?",(joinable_key,rowid))
        conn.commit()
    except sqlite3.OperationalError as e:
        print(f"skipping {table} because {e}")

for table in tables_names:
    try:
        cur.execute(f"SELECT * FROM {table} LIMIT 5")
        rows = cur.fetchall()
        print(f"sample data from {table} with joinable key: ")
        for row in rows:
            print(row)
    except sqlite3.OperationalError as e:
        print(f"skipping {table} due to {e}")


conn.commit()
conn.close()
print("data has transferred into SQL Successfully")


