import os
from dotenv import load_dotenv, find_dotenv
from pg8000 import Connection
import csv
load_dotenv(find_dotenv())
tote_user= os.environ.get("tote_user")
tote_password= os.environ.get("tote_password")
host='nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com'
database='totesys'
user=tote_user
password=tote_password
port=5432
conn = Connection(user=user, password=password, host=host, database=database, port = port)
table_query = 'SELECT table_name FROM information_schema.tables WHERE table_schema = \'public\';'
table_names = conn.run(table_query)
for table_name in table_names:
    query = f"SELECT * FROM {table_name[0]}"
    table_data = conn.run(query)
    with open(f"{table_name[0]}.csv", "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(table_data)
print(table_names)
conn.close()