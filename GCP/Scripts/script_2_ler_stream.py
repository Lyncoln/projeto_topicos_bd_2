from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
import os
from pyspark.sql.functions import col
from google.cloud import storage

spark = (SparkSession
             .builder
             .appName("streaming")
             .getOrCreate()
             )

schema = StructType([
        StructField("Datetime", StringType(), True),
        StructField("MW", DoubleType(), True)
    ])

streaming = (
            spark.readStream.schema(schema)
            .csv("gs://dataproc-staging-us-east1-477751747254-kpkmtkpj/notebooks/jupyter/dados_brutos/",header=True)
)


streaming_query = (streaming
                  .withColumn("Datetime",col("Datetime")[0:10])
                  .writeStream
                  .format('csv')
                  .option(key = "path", value = "gs://dataproc-staging-us-east1-477751747254-kpkmtkpj/notebooks/jupyter/dados_stream/")
                  .option(key = "checkpointLocation", value = "gs://dataproc-staging-us-east1-477751747254-kpkmtkpj/notebooks/jupyter/checkpoint/")
                  .outputMode('append')
                  .start())