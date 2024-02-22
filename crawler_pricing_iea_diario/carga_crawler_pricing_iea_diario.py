"""
@author: paulolima
"""

from boto.s3.connection import S3Connection
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta.tables import *   
import boto3


# create Spark session
spark = (SparkSession.builder.appName("Pyspark - Staging iea_diario")
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") 
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") 
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    .getOrCreate())  

# BUCKET NAME DELTA TABLES
BUCKET_NAME = "XXXXXXXXXXXXX336967"
CONSUMER_ZONE = f's3://XXXXXXXXXXX080336967/consumer-zone/crawlers/pricing/iea_diario/fat/'

# coletar o ultimo arquivo modificado 
s3_resource = boto3.resource('s3')
objects = list(s3_resource.Bucket(BUCKET_NAME).objects.filter(Prefix='raw-zone/crawlers/pricing/iea_diario/fat/'))
objects.sort(key=lambda o: o.last_modified)
ultimo_arquivo = objects[-1].key


# read raw file iea_diario in raw_zone
csv_file = (spark.read.option('delimiter', ',').option('header', 'true').option("encoding", "ISO-8859-15").csv(f's3://{BUCKET_NAME}/{ultimo_arquivo}'))
          
normalized_columns = []

for col in csv_file.columns:
  col = col.lower()
  col = col.strip()
  normalized_columns.append(col)
df_normalizado = csv_file.toDF(*(normalized_columns))

deltaTable = DeltaTable.forPath(spark, CONSUMER_ZONE)

deltaTable = deltaTable.alias("lake").merge(
    df_normalizado.alias("carga"),
    "lake.id_coleta = carga.id_coleta") \
  .whenMatchedUpdateAll() \
  .whenNotMatchedInsertAll() \
  .execute()
  
# PASSANDO O CAMINHO ONDE FOI SALVO O ARQUIVO DELTA
delta = DeltaTable.forPath(spark, CONSUMER_ZONE)

# DELETANDO O HISTORICO ANTIGO
delta.vacuum()

# GERANDO O LINK DE CONEXÃO COM O ATHENA
delta.generate("symlink_format_manifest")

spark.stop()