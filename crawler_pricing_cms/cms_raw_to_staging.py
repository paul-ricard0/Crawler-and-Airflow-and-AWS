from datetime import datetime
from time import time
import re, os, io

import pandas as pd
import tabula # Ler o pdf
from tabula.io import read_pdf

# AWS
import boto3

# Variáveis de ambiente
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

# Criando a client do Boto3
s3_client = boto3.client(
    's3', 
    region_name='us-east-1',
    aws_access_key_id=AWS_ACCESS_KEY_ID, 
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY 
)

date_moth = datetime(datetime.today().year, datetime.today().month, 1) # dia 1 do mês atual
date_moth_str = date_moth.strftime('%Y-%m-%d')
BUCKET_NAME = 'CCCCCCCCCCC0336967'
KEY_PDF_RAW = f'raw-zone/crawlers/pricing/cms/pdf/cms_{date_moth_str}.pdf'
ARQUIVO_LOCAL = f'cms_{date_moth_str}.pdf' 
KEY_PARQUET_STAGING = f'staging-zone/crawlers/pricing/cms/fat/year={date_moth:%Y}/month={date_moth:%m}/cms_{date_moth_str}.parquet'


def hora_atual(): # Função pra retornar a hora atual
    return datetime.fromtimestamp(time()).strftime('%Y-%m-%d %H:%M:%S')

print('\nFazendo download do pdf na raw-zone...', hora_atual())
s3_client.download_file(BUCKET_NAME, KEY_PDF_RAW, ARQUIVO_LOCAL)

print('\nLendo pdf...', hora_atual() )
cms = tabula.read_pdf(ARQUIVO_LOCAL,
                     pages='2',
                     encoding="ISO-8859-15",
                     pandas_options={'header': None},
                     stream=True,
                     guess=False)[0]
cms.columns = ['dados'] 

print('\nCorrigindo as colunas indevidas e filtrando somente o C.M.S...', hora_atual())
cms = cms.set_index(cms['dados']).filter(like='C.M.S( C ) ', axis= 0)
cms = re.sub("BARRIGA","",str(cms))
cms = cms[249:287]
cms = cms.replace(',','.')
cms = cms.replace('/',' ')
cms = cms.replace('( C )','')
cms = cms.replace(' ',';')

print('Gerando o dataframe...', hora_atual())
df = pd.DataFrame([x.split(';') for x in cms.split('\n')])
df.columns = ['materia_prima', 'sp', 'sp_congelado','rj','rj_congelado']
df['data'] = date_moth_str
df['ultima_atualizacao'] = hora_atual()
print('Base gerada...', hora_atual())

print('Criando espaço na memória...', hora_atual())
out_buffer = io.BytesIO() 
print('Salvando na memória em formato parquet', hora_atual())
df.to_parquet(out_buffer, index=False) 

print('Subindo parquet para a staging-zone...', hora_atual())
s3_client.put_object(
    Body=out_buffer.getvalue(),
    Bucket=BUCKET_NAME, 
    Key=KEY_PARQUET_STAGING 
)
print('Upload finalizado com sucesso!!!', hora_atual())
