import datetime
from google.cloud import bigquery
import pandas
import pytz
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "skilled-mark-320102-958694fbf5ca.json"

# BigQuery iniciar cliente
client = bigquery.Client()

table_id = "skilled-mark-320102.fabiobigquery.dados_processados"

job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("ds", bigquery.enums.SqlTypeNames.DATETIME),
        bigquery.SchemaField("y", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("yhat", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("rmse", bigquery.enums.SqlTypeNames.FLOAT64),
    ],
    write_disposition="WRITE_TRUNCATE",
    #se ativar, substitui a tabela toda, ao em vez de dar append
)

job = client.load_table_from_dataframe(
    dataframe, table_id, job_config=job_config
)
job.result()

table = client.get_table(table_id)  # Make an API request.
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)