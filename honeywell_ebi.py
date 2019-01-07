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

result = pd.read_sql("SELECT PVValue, PointID FROM AnalogPoint WHERE PointID in ('S04_PAU02_CO2','S04_PAU02_FA_Temp','S04_PAU02_SAT_Sp','S04_PAU02_SA_Temp','S30_PAU01_SA_Temp','S30_PAU02_SA_Temp','S15_PAU01_SA_Temp','S15_PAU02_SA_Temp','S26_AHU01_Press','S26_AHU01_PressSp','S26_AHU01_RAT','S26_AHU01_SAT','S26_AHU01_SAT_Sp','S26_AHU02_Press','S26_AHU02_PressSp','S26_AHU02_RAT','S26_AHU02_SAT','S26_AHU02_SAT_Sp','S26_AHU03_Press','S26_AHU03_PressSp','S26_AHU03_RAT','S26_AHU03_SAT','S26_AHU03_SAT_Sp','S26_AHU04_Press','S26_AHU04_PressSp','S26_AHU04_RAT','S26_AHU04_SAT','S26_AHU04_SAT_Sp','S46_AHU01_Press','S46_AHU01_PressSp','S46_AHU01_RAT','S46_AHU01_SAT','S46_AHU01_SAT_Sp','S46_AHU02_Press','S46_AHU02_PressSp','S46_AHU02_RAT','S46_AHU02_SAT','S46_AHU02_SAT_Sp','S46_AHU03_Press','S46_AHU03_PressSp','S46_AHU03_RAT','S46_AHU03_SAT','S46_AHU03_SAT_Sp','S46_AHU04_Press','S46_AHU04_PressSp','S46_AHU04_RAT','S46_AHU04_SAT','S46_AHU04_SAT_Sp') ", cnxn)
result = result.set_index('PointID')
result = result.T

result ['time'] = now.isoformat('T') + 'Z'
result = result.set_index('time')
result = result.rename(columns=lambda x: x.strip())

#result.to_csv('data_' + str(now.strftime('%Y-%m-%d %H%M')) + '.csv')

data_output = result.to_csv()
files = {'file': ('data_' + str(now.strftime('%Y-%m-%d %H%M')) + '.csv', data_output)}
res = requests.post('http://172.16.0.1:8080/io/v1/negaports/5c2c8a713a7aad0d025e0db6/data?token=f36ce3b025aab778b0c0e10018d38361', files=files)

#print(res.status_code)
#print(res.text)

result = pd.read_sql("SELECT PVValue, PointID FROM StatusPoint WHERE PointID in ('S04_PAU02_Status','S30_PAU01_sta','S30_PAU02_sta','S15_PAU01_sta','S15_PAU02_sta','S26_AHU01_sta','S26_AHU02_sta','S26_AHU03_sta','S26_AHU04_sta','S46_AHU01_sta','S46_AHU02_sta','S46_AHU03_sta','S46_AHU04_sta')", cnxn)
result = result.set_index('PointID')
result = result.T

result ['time'] = now.isoformat('T') + 'Z'
result = result.set_index('time')
result = result.rename(columns=lambda x: x.strip())

#result.to_csv('data_' + str(now.strftime('%Y-%m-%d %H%M')) + '.csv')

data_output = result.to_csv()
files = {'file': ('data_' + str(now.strftime('%Y-%m-%d %H%M')) + '.csv', data_output)}
res = requests.post('http://172.16.0.1:8080/io/v1/negaports/5c2c8a713a7aad0d025e0db6/data?token=f36ce3b025aab778b0c0e10018d38361', files=files)

#print(res.status_code)
#print(res.text)