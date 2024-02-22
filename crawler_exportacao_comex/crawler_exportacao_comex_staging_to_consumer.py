print('Iniciando tarefa de evolução de arquivos da pipeline crawler-exportacao-comexstat da staging-zone para consumer-zone.')
import boto3
import os
from datetime import datetime

hoje =  datetime.today().date()


AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")


BUCKET_NAME = "XXXX548080336967"
KEY_STAGING_PREFIX = f'staging-zone/crawlers/exportacao/comexstat/fat/year={hoje:%Y}/month={hoje:%m}/day={hoje:%d}'
KEY_CONSUMER = f'consumer-zone/crawlers/exportacao/comexstat/fat/year={hoje:%Y}/month={hoje:%m}/day={hoje:%d}/comexstat_{hoje:%Y%m%d}.parquet'

KEY_MANIFEST=f'consumer-zone/crawlers/exportacao/comexstat/fat/_symlink_format_manifest/manifest_{hoje:%Y}'

# copiar o ultimo arquivo da staging para consumer 
s3_client=boto3.client(
    's3', 
    region_name='us-east-1',
    aws_access_key_id=AWS_ACCESS_KEY_ID, 
    aws_secret_access_key= AWS_SECRET_ACCESS_KEY
)

print('Buscando caminho do arquivo na staging-zone...')
ultimo_arquivo=s3_client.list_objects(
    Bucket=BUCKET_NAME,
    Prefix=KEY_STAGING_PREFIX
)['Contents'][-1]['Key'] #lista arquivos na pasta e pega a chave (caminho) do último (mais recente)

print('Copiando objeto da staging-zone para consumer-zone...')
response = s3_client.copy_object(
    Bucket=BUCKET_NAME,
    CopySource=BUCKET_NAME+'/'+ultimo_arquivo,
    Key=KEY_CONSUMER,
)

print('Buscando conteúdo do Manifest...')
manifest_obj = s3_client.get_object(
    Bucket=BUCKET_NAME,
    Key=KEY_MANIFEST,
)

print('Apendando caminho do novo arquivo de dados ao conteúdo do Manifest...')
manifest_contents_new = '\ns3://' +BUCKET_NAME+'/'+KEY_CONSUMER

print('Colocando novo Manifest no S3...')
response_manifest = s3_client.put_object(
    Body=manifest_contents_new,
    Bucket=BUCKET_NAME,
    Key=KEY_MANIFEST,
)

print('Tarefa de evolução de comexstat da staging-zone para consumer-zone finalizada.')
