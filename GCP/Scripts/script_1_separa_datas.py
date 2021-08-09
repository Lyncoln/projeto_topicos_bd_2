import os
import pandas as pd
from google.cloud import storage

client = storage.Client()
for file in client.list_blobs('dataproc-staging-us-east1-477751747254-kpkmtkpj', prefix='notebooks/jupyter/data'):
    num = file.name.count('/')
    string = file.name.split('/')[num]
    name_folder = string.replace("_hourly.csv","")
    if string != "":
        arquivo = "gs://dataproc-staging-us-east1-477751747254-kpkmtkpj/notebooks/jupyter/data/" + string
        data = pd.read_csv(arquivo, encoding='utf-8')
        print((data.head()))
        print(data["Datetime"])
        data['day']=data["Datetime"].astype('datetime64[ns]').dt.floor('d')
        for name,group in data.groupby('day'):
            group[["Datetime",f'{name_folder}_MW']].to_csv(f'gs://dataproc-staging-us-east1-477751747254-kpkmtkpj/notebooks/jupyter/bases/{name_folder}/date{str(name)[0:10]}.csv',index = False)


client = storage.Client() 
bucket = client.bucket('dataproc-staging-us-east1-477751747254-kpkmtkpj')
blobs = list(bucket.list_blobs(prefix='notebooks/jupyter/data'))

for file in client.list_blobs('dataproc-staging-us-east1-477751747254-kpkmtkpj', prefix='notebooks/jupyter/data'):
    num = file.name.count('/')
    string = file.name.split('/')[num]

    print(file)