import os
from datetime import datetime, timedelta
from time import sleep
import pandas as pd
from google.cloud import storage
import io

try:
    data = datetime.strptime(os.environ["data"], '%Y-%m-%d')
    data_str = str(data)[0:10]
    
except:
    data_str = "2004-10-01"
    os.environ["data"] = data_str
    
q_days = int(input("Quantos dias para simular\n"))
client = storage.Client()


for day in range(q_days):
    for file in client.list_blobs('dataproc-staging-us-east1-477751747254-kpkmtkpj', prefix='notebooks/jupyter/bases'):
      if(data_str in file.name):
        blob = bucket.blob(file.name)
        dados = blob.download_as_string()
        df = pd.read_csv(io.BytesIO(dados))
        df_2 = df.rename(columns = {df.columns[0]:"Datetime",
                                              df.columns[1]:"MW"})
        name = str(file.name).split("/")
        bucket = client.get_bucket('dataproc-staging-us-east1-477751747254-kpkmtkpj')
        destino = 'notebooks/jupyter/dados_brutos/' + name[3] + "_" +  name[4]
        bucket.blob(destino).upload_from_string(df_2.to_csv(), 'text/csv')

new_data = str(datetime.strptime(os.environ["data"], '%Y-%m-%d') + timedelta(1))[0:10]
os.environ["data"] = new_data