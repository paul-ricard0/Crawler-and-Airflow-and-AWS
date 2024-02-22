print('Iniciando tarefa de evolução de arquivos da pipeline crawler-pricing-iea-diario da raw-zone para staging-zone.')
import boto3
import pandas as pd
import os
import io
from datetime import datetime

hoje =  datetime.today().date()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")

BUCKET_NAME = "a3data-548080336967"
KEY_PREFIX_RAW = 'raw-zone/crawlers/pricing/iea_diario/fat/'
KEY_STAGING = f'staging-zone/crawlers/pricing/iea_diario/fat/year={hoje:%Y}/month={hoje:%m}/day={hoje:%d}/iea_diario_{hoje:%Y%m%d}.parquet'

# coletar o ultimo arquivo modificado 
s3 = boto3.client('s3')

print('buscando caminho do arquivo na raw-zone...')

lista_arquivos=s3.list_objects( 
    Bucket=BUCKET_NAME,
    Prefix=KEY_PREFIX_RAW
)['Contents']

# selecionamos a chave do primeiro item na lista de arquivos na pasta raw em ordem decrescente por data de modificação
ultimo_arquivo = sorted(lista_arquivos, key=lambda d: d['LastModified'], reverse=True)[0]['Key']

print('lendo o arquivo para um DataFrame')
df = pd.read_csv(
    f"s3://{BUCKET_NAME}/{ultimo_arquivo}",
    storage_options={
        "key": AWS_ACCESS_KEY_ID,
        "secret": AWS_SECRET_ACCESS_KEY,
        "token": AWS_SESSION_TOKEN,
    },
    encoding="ISO-8859-15" # encoding escolhido na etapa de escrita na raw -pq?
)

print('tratando nomes de colunas do DataFrame...')
normalized_columns = df.columns.str.lower().str.strip()
df.columns = normalized_columns


print('salvando DataFrame em parquet no buffer...')
out_buffer = io.BytesIO()
df.to_parquet(out_buffer, index=False)

print('salvando parquet na staging-zone...')
s3.put_object(Bucket=BUCKET_NAME, Key=KEY_STAGING, Body=out_buffer.getvalue())


print('Tarefa de evolução de iea-diario da raw-zone para staging-zone finalizada.')
