import pandas as pd
import sqlite3

data_directory = "data"
output_data = "output_data"

conn = sqlite3.connect("crypto_data.db") # connect to SQLite 3 Database

