
from datetime import datetime
from time import time, sleep

import pandas as pd
import os
# import requests
import io
import ssl
import boto3
import awswrangler as wr
aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']

session = boto3.Session(
    region_name = "us-east-1",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

ssl._create_default_https_context = ssl._create_unverified_context


hoje = datetime.today().date()


ano  = hoje.year


if datetime.now().month == 1:
    currentYear = datetime.now().year-1
    currentMonth = datetime.now().month+11
else:
    currentYear = datetime.now().year
    currentMonth = datetime.now().month-1



link = 'https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/EXP_'+ str(ano) +'.csv'
print (link)
df = pd.read_csv(link,delimiter = ';',on_bad_lines='skip')


if(df.columns[0] != 'CO_ANO') :
    link = 'https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/EXP_'+ str(ano-1) +'.csv'
    print (link)
    df = pd.read_csv(link,delimiter = ';',on_bad_lines='skip')

csv_buffer = io.StringIO()
df.to_csv(csv_buffer)
csv_buffer.seek(0)


nome_raw = datetime.now().strftime('%Y%m%d')+f'EXP_{currentYear}_{currentMonth}.csv'

print(nome_raw)

ano_raw =  currentYear




print('Query to Athena...')
try:
    os.environ["AWS_DEFAULT_REGION"] = 'us-east-1'

    df_athena = wr.athena.read_sql_query("""
    SELECT MAX(CAST(CONCAT(cast(co_ano as varchar), '-', cast(co_mes as varchar), '-01') AS DATE)) AS current_largest_date
    FROM "consumer_zone"."comex_fat"
    """, database='consumer_zone')
except Exception as e:
    raise ValueError(e)




current_largest_date = pd.to_datetime(df_athena['current_largest_date'][0])
print(f'A data mais recente no Lake é {current_largest_date}.')

df['new_largest_date'] = pd.to_datetime(df['CO_ANO'].astype(str) + '-' +df['CO_MES'].astype(str) + '-01')

if  df.loc[df['new_largest_date'] > current_largest_date].empty:
    print("Ano e mês já existem no Lake.")    
else:
    with io.BytesIO() as buffer:
        df.to_csv(buffer)
        buffer.seek(0)    
        wr.s3.upload(
            local_file=buffer, 
            path = f's3://XXXXXX48080336967/raw-zone/crawlers/exportacao/comexstat/fat/{nome_raw}', 
            boto3_session = session
            )
        print(f'Upload do arquivo {nome_raw} gravado com sucesso!')