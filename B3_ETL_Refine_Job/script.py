import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

input_path = "s3://parquet-files-870946032395/raw/"

df = spark.read.parquet(input_path)

df = df.withColumn("date", F.from_unixtime(F.col("date")).cast("date"))

df_renamed = df.withColumnRenamed("close", "preco_fechamento") \
               .withColumnRenamed("volume", "volume_negociado") \
               .withColumnRenamed("date", "data_referencia")

windowSpec = Window.partitionBy("ticker").orderBy("data_referencia").rowsBetween(-4, 0)
df_with_metrics = df_renamed.withColumn("media_movel_5d", F.avg("preco_fechamento").over(windowSpec))

df_refined = df_with_metrics.groupBy("ticker", "data_referencia", "data_ingestao") \
    .agg(
        F.first("preco_fechamento").alias("preco_fechamento"),
        F.sum("volume_negociado").alias("volume_total_dia"),
        F.max("high").alias("maxima_dia"),
        F.min("low").alias("minima_dia"),
        F.first("media_movel_5d").alias("media_movel_fechamento")
    )

output_path = "s3://parquet-files-870946032395/refined/"
df_refined.write.mode("overwrite") \
    .partitionBy("data_ingestao", "ticker") \
    .parquet(output_path)

job.commit()

glue_client = boto3.client('glue')
crawler_name = 'crawler-b3-refined'

try:
    glue_client.start_crawler(Name=crawler_name)
    print(f"Crawler {crawler_name} iniciado com sucesso!")
except Exception as e:
    print(f"Erro ao iniciar o crawler: {e}")

job.commit()