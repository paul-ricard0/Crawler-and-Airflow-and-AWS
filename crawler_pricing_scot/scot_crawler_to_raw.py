from datetime import datetime
import os

import requests
from bs4 import BeautifulSoup as bs

import awswrangler as wr
import boto3

# Variáveis de ambiente
aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']

# Criando a sessão do Boto3
session = boto3.Session(
    region_name = "us-east-1",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

# declara o link
URL = 'https://www.scotconsultoria.com.br/cotacoes/'

def upload_raw_zone(indicador: str, pagina: bs):
    '''Salva a página html localmente e faz o upload do arquivo .html para raw-zone
    principal motivo é o backup, caso tenha problema no crawler vamos ter na raw-zone o dado do dia 
    '''
    
    # Dando nome ao arquivo html 
    nome_arq= datetime.now().strftime('%Y-%m-%d')+f'scot_{indicador}.html'

    # Escrevendo o html localmente
    with open(nome_arq, "w") as file:
        file.write(str(pagina))
        
    print(f'Gerando arquivo.html SCOT_{indicador.upper()}  na raw-zone...')
    wr.s3.upload(
        local_file = nome_arq,
        path = f's3://XXXXXXXXXXXXXXX080336967/raw-zone/crawlers/pricing/scot/{indicador}/fat/{nome_arq}',
        boto3_session = session,
    )
    print(f'Upload do arquivo.html SCOT_{indicador.upper()} na raw feito com sucesso!\n')
    
    return nome_arq

if __name__ == '__main__':
    indicadores = ['boi-gordo', 'atacado']
    
    for indicador in indicadores:
        # request código html
        res = requests.get(URL+f'{indicador}/')
        html_doc = res.text

        # estrutura de dados aninhada
        soup = bs(html_doc, 'html.parser')

        html = upload_raw_zone(indicador=indicador, pagina=soup)