from urllib.request import Request, urlopen
from bs4 import BeautifulSoup

from datetime import datetime
import os

import awswrangler as wr
import boto3

# Credenciais AWS
AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')

# Criando a sessão do Boto3
session = boto3.Session(
    region_name="us-east-1",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

def hora_atual() -> str:
    """Retorna a hora atual no formato '%Y-%m-%d %H:%M:%S'"""
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def get_pagina(url: str) -> BeautifulSoup:
    """Faz um request na página e retorna o objeto BeautifulSoup"""
    print(f'Fazendo request na página... {hora_atual()}')
    hdr = {'User-Agent': 'Mozilla/5.0'}
    requests = Request(url, headers=hdr)
    
    print(f'Lendo html da página... {hora_atual()}')
    html = urlopen(requests)
    pagina = BeautifulSoup(html, "html.parser")
    return pagina

def write_file(pagina: BeautifulSoup, indicador: str) -> str:
    """Escreve o html da página em um arquivo local e retorna o caminho do arquivo"""
    print(f'Escrevendo html localmente... {hora_atual()}')
    arquivo_local = f'acss_{indicador}_{datetime.now().strftime("%Y-%m-%d")}.html'
    
    with open(arquivo_local, "w") as file:
        file.write(str(pagina))
    return arquivo_local

def upload_to_raw(arquivo: str, indicador: str) -> None:
    """Faz upload do arquivo para a raw-zone"""
    print(f'Fazendo upload html para a raw-zone... {hora_atual()}')
    wr.s3.upload(
        local_file=arquivo,
        path=f's3://XXXXXXXX548080336967/raw-zone/crawlers/pricing/acss/{indicador}/{datetime.now().strftime("%Yy-%mm")}/{arquivo}',
        boto3_session=session,
    )
    print(f'{indicador.upper()} finalizado com SUCESSO!!!\n')

if __name__ == '__main__':
    indicadores = {
        'graos': 'https://accs.org.br/cotacoes/mercado-de-graos/',
        'suinos_base': 'https://accs.org.br/cotacoes/suino-preco-base/',
        'suinos_bolsa': 'https://accs.org.br/cotacoes/bolsa-de-suinos/'
    }
    
    for indicador, url_indicador in indicadores.items():
        print(indicador.upper(), hora_atual())
        pagina = get_pagina(url_indicador)
        arquivo = write_file(pagina, indicador)
        upload_to_raw(arquivo, indicador)
