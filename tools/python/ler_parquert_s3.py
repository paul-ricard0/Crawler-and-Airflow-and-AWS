import pandas as pd
import io
import os
from datetime import datetime
# AWS
import boto3

# Credenciais aws
AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"] 

#Configura a sessão do client da aws
s3_resource = boto3.resource(
    's3',
    region_name = "us-east-1",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY)


# Bucket e Key (caminho) do arquivo no S3
BUCKET_NAME='X6967'
path_parquet = f'X/acss/suinos_base/fat/year=2023/month=03/day=24/acss_suinos_base_2023-03-24.parquet'



# criando espaço na mémoria para salvar o objeto
buffer_for_parquet = io.BytesIO()

#Coletando dados da raw-zone e reconstruindo o json
objeto = s3_resource.Object(BUCKET_NAME,path_parquet)
objeto.download_fileobj(buffer_for_parquet)

# lendo o parquet em formato de dataframe
df = pd.read_parquet(buffer_for_parquet)

print(df)

