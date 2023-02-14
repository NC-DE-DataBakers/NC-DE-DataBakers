import os
from dotenv import load_dotenv, find_dotenv
from pg8000 import Connection
import csv
load_dotenv(find_dotenv())

host='nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com'
database='totesys'
user=os.environ.get("tote_user")
password=os.environ.get("tote_password")

conn = Connection(user=user, password=password, host=host, database=database)
table_query = 'SELECT table_name FROM information_schema.tables WHERE table_schema = \'public\';'
table_names = conn.run(table_query)
if not os.path.exists('DB/data'):
    os.makedirs('DB/data')
for table_name in table_names:
    query = f"SELECT * FROM {table_name[0]}"
    table_data = conn.run(query)
    with open(f"DB/data/{table_name[0]}.csv", "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(table_data)

conn.close()