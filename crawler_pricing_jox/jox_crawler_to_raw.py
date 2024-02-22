from datetime import datetime 
from time import time
import requests
import os

import boto3
import awswrangler as wr


# Credenciais aws
aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']

# Configura a sessão do client da aws
SESSION = boto3.Session(
    region_name = "us-east-1",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

# Link do Pdf Diário 
URL = 'https://www.jox.com.br/sistema/noticias/boletim/diario.pdf'

data_atual = datetime.fromtimestamp(time()).strftime('%Y-%m-%d-%Hh-%Mm')
nome_pdf = data_atual+'.pdf'

print('Requisitando o boletim diario...')
response = requests.get(URL)

print('Salvando o arquivo localmente...') 
with open(nome_pdf, 'wb') as f:
    f.write(response.content)

try:  
    print('Fazendo o upload do arquivo PDF para backup NA RAW-ZONE')
    wr.s3.upload( 
        local_file=nome_pdf, 
        path = f's3://XXXXXXXXXXXXXXXX80336967/raw-zone/crawlers/pricing/jox/pdf/{nome_pdf}', 
        boto3_session = SESSION
    )
    print(f'Upload do arquivo PDF diário {nome_pdf} gravado com sucesso!') 
except:
    print('Erro ao fazer upload do arquivo PDF!!!')