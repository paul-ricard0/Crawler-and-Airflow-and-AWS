from datetime import datetime
from time import time
import os

# Lib google drive
import gdown

# AWS
import boto3
import awswrangler as wr

# Variáveis de ambiente
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

# Criando a sessão do Boto3
session = boto3.Session(
    region_name = "us-east-1",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

URL='https://drive.google.com/file/d/XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'

data_mes = datetime(datetime.today().year, datetime.today().month, 1).strftime('%Y-%m-%d') # dia 1 do mês atual
nome_pdf = 'cms_'+data_mes+'.pdf'

# Função pra retornar a hora atual
def hora_atual():
    return datetime.fromtimestamp(time()).strftime('%Y-%m-%d %H:%M:%S')

print('\nFazendo download PDF do drive...', hora_atual())
gdown.download(URL, nome_pdf, quiet=False, fuzzy=True, verify=False)

print('Fazendo o upload do arquivo PDF para backup NA RAW-ZONE...', hora_atual())
wr.s3.upload( 
    local_file=nome_pdf, 
    path = f's3://XXXXXXXXXXXXXXXXX080336967/raw-zone/crawlers/pricing/cms/pdf/{nome_pdf}', 
    boto3_session = session
)
print('Upload do arquivo PDF gravado na raw-zone com sucesso!!!', hora_atual()) 