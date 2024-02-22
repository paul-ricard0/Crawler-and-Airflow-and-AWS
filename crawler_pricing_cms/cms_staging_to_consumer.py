from datetime import datetime
from time import time
import os

import boto3

# Credenciais aws
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY'] 

date_moth = datetime(datetime.today().year, datetime.today().month, 1) # data do mês com dia 01
BUCKET_NAME = 'xxxxxxxxxxxxxxxx336967'
KEY_MANIFEST ='consumer-zone/crawlers/pricing/cms/fat/_symlink_format_manifest/manifest'
KEY_PARQUET_CONSUMER = f'consumer-zone/crawlers/pricing/cms/fat/year={date_moth:%Y}/month={date_moth:%m}/cms_{date_moth.strftime("%Y-%m-%d")}.parquet'
KEY_PARQUET_STAGING = f'staging-zone/crawlers/pricing/cms/fat/year={date_moth:%Y}/month={date_moth:%m}/cms_{date_moth.strftime("%Y-%m-%d")}.parquet'

def hora_atual(): # Função pra retornar a hora atual
    return datetime.fromtimestamp(time()).strftime('%Y-%m-%d %H:%M:%S')

print('Criando a client do Boto3', hora_atual())
s3_client=boto3.client(
    's3', 
    region_name='us-east-1',
    aws_access_key_id=AWS_ACCESS_KEY_ID, 
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY 
)

print('Copiando parquet da staging para consumer...', hora_atual())
response = s3_client.copy_object(
    Bucket=BUCKET_NAME,
    CopySource=BUCKET_NAME+'/'+KEY_PARQUET_STAGING,
    Key=KEY_PARQUET_CONSUMER
)

print('Atualizando Manifest para ligação com Athena...', hora_atual())
manifest_obj = s3_client.get_object(
    Bucket=BUCKET_NAME,
    Key=KEY_MANIFEST,
)

manifest_contents = manifest_obj['Body'].read( ).decode('utf-8')
manifest_contents_new = manifest_contents + '\ns3://' +BUCKET_NAME+'/'+KEY_PARQUET_CONSUMER

print('Sobreescrevendo novo manifest...', hora_atual())
response_manifest = s3_client.put_object(
    Body=manifest_contents_new,
    Bucket=BUCKET_NAME,
    Key=KEY_MANIFEST,
)
print('Finalizado com sucesso!!!', hora_atual())