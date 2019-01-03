import datetime
import json
import pyodbc
import pandas as pd
import requests

pyodbc.pooling = False

with open('./config.json', 'r') as f:
    config = json.load(f)

print(pyodbc.drivers())

cnxn = pyodbc.connect(config['connString'])
df = pd.read_sql("SELECT * FROM [test].[dbo].[data]", cnxn)
df = df.set_index('timestamp')
# test = df.T
# print(test.index)
df.to_csv('data_' + str(datetime.datetime.now().timestamp()) + '.csv')

# cursor = cnxn.cursor()
# cursor.execute("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")
# rows = cursor.fetchall()
# print(rows)