import datetime
import json
import pyodbc
import pandas as pd
import requests
import os

pyodbc.pooling = False

with open(os.path.join(os.path.dirname(__file__), 'config.json'), 'r') as f:
    config = json.load(f)

cnxn = pyodbc.connect(config['connString'], autocommit=True)
cursor = cnxn.cursor()
now = datetime.datetime.utcnow()

result = pd.read_sql("SELECT TimeStamp, Value001, Quality001 FROM History6MinAverage WHERE Parameter001 = 'TankLevel.pv'", cnxn)

result.to_csv('data_' + str(now.strftime('%Y-%m-%d %H%M')) + '.csv')
