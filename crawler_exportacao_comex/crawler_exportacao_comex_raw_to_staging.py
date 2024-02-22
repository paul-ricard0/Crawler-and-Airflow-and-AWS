print('Iniciando tarefa de evolução de arquivos da pipeline crawler-exportacao-comex-stat da raw-zone para staging-zone.')
import boto3
import pandas as pd
import os
import io
from datetime import datetime
import time

hoje =  datetime.today().date()

hora_atual = datetime.today()


AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
# AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")

BUCKET_NAME = "a3data-548080336967"
KEY_PREFIX_RAW = 'raw-zone/crawlers/exportacao/comexstat/fat/'
KEY_STAGING = f'staging-zone/crawlers/exportacao/comexstat/fat/year={hoje:%Y}/month={hoje:%m}/day={hoje:%d}/comex_stat_{hoje:%Y%m%d}.parquet'

# coletar o ultimo arquivo modificado 
s3 = boto3.client('s3')

print('buscando caminho do arquivo na raw-zone...')

lista_arquivos=s3.list_objects( 
    Bucket=BUCKET_NAME,
    Prefix=KEY_PREFIX_RAW
)['Contents']

ultimo_arquivo = lista_arquivos[-1]['Key']

print('lendo o arquivo para um DataFrame')
df = pd.read_csv(
    f"s3://{BUCKET_NAME}/{ultimo_arquivo}",
    storage_options={
        "key": AWS_ACCESS_KEY_ID,
        "secret": AWS_SECRET_ACCESS_KEY
        # "token": AWS_SESSION_TOKEN,
    },
    sep=",",
    encoding="ISO-8859-15" # encoding escolhido na etapa de escrita na raw -pq?
)
print('reordenando as colunas')
print(df)
co_ano = df['CO_ANO']
co_mes = df['CO_MES']
df = df.drop(columns=['CO_ANO','CO_MES'])
len = len(df.columns)
df.insert(len,column='CO_ANO', value=co_ano)
df.insert(len+1,column='CO_MES', value=co_mes)


print('tratando nomes de colunas do DataFrame...')
normalized_columns = df.columns.str.lower().str.strip()
df.columns = normalized_columns

df.drop(columns=['unnamed: 0','new_largest_date'], inplace = True)
df = df.astype({"co_ncm" : str,"co_pais": str,"co_via": str,"co_urf":str, "co_pais": int})
print(hora_atual)
df['ultima_atualizacao'] = hora_atual
print(df)

print('salvando DataFrame em parquet no buffer...')
out_buffer = io.BytesIO()
df.to_parquet(out_buffer, index=False)

print('salvando parquet na staging-zone...')
s3.put_object(Bucket=BUCKET_NAME, Key=KEY_STAGING, Body=out_buffer.getvalue())


print('Tarefa de evolução de comexstat da raw-zone para staging-zone finalizada.')
