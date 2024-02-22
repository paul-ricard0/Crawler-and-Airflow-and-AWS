from boto.s3.connection import S3Connection
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta.tables import *   
import boto3

if __name__ == '__main__':

      # create Spark session
      spark = (SparkSession.builder.appName("Pyspark - Staging ceagesp")
          .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0")
          .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") 
          .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") 
          .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
          .getOrCreate())  
      
      print('# BUCKET NAME DELTA TABLES')
      BUCKET_NAME = "XXXXXX8080336967"
      CONSUMER_ZONE = f"s3://XXXXX8080336967/consumer-zone/crawlers/pricing/ceagesp/fat/"
      
      
      print('# coletar o ultimo arquivo modificado ')
      s3_resource = boto3.resource('s3')
      objects = list(s3_resource.Bucket(BUCKET_NAME).objects.filter(Prefix='staging-zone/crawlers/pricing/ceagesp/fat/'))  
      objects.sort(key=lambda o: o.last_modified)
      ultimo_arquivo = objects[-1].key

      
      print('# read raw file  in raw_zone')
      csv_file = (spark.read.option('delimiter', ',').option('header', 'true').option("encoding", "UTF-8").csv(f's3://{BUCKET_NAME}/{ultimo_arquivo}'))
                
      normalized_columns = []

      for col in csv_file.columns:
        col = col.lower()
        col = col.strip()
        normalized_columns.append(col)
      df_with_normalized_columns = csv_file.toDF(*(normalized_columns))
      
      print('# overwrite date into delta table ')
      df_with_normalized_columns.write.mode("append").format("delta").save(CONSUMER_ZONE)

      
      print('# delete historical delta files')
      delete_historical = DeltaTable.forPath(spark, CONSUMER_ZONE)
      delete_historical.vacuum()
      
      print('# generate manifest to connect to Athena')
      delete_historical.generate("symlink_format_manifest")
      
      print("TABELA ESCRITA NA CONSUMER-ZONE COM SUCESSO!!!")
      # stop Spark session
      spark.stop()
