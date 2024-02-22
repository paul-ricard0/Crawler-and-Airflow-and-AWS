from urllib.request import Request, urlopen
from bs4 import BeautifulSoup as bs

from datetime import datetime
from time import time
import os

import pandas as pd

# AWS
import boto3
import awswrangler as wr

# Credenciais aws
aws_access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"] 

# Configura a sessão do client da aws
session = boto3.Session(
    region_name = "us-east-1",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

# Link do site com os indicadores
URL = "https://www.cepea.esalq.usp.br/br/indicador/"

# Variável para retornar o dia de hoje
date_today = datetime.today().date()

def hora_atual():
    ''' Função pra retornar a hora atual '''
    return datetime.fromtimestamp(time()).strftime('%Y-%m-%d %H:%M:%S')

def get_pagina(indicador: str):
    ''' retornar um BeautifulSoup.HTML '''
    hdr = {'User-Agent': 'Mozilla/5.0'}
    requests = Request(URL+indicador+".aspx", headers=hdr)

    html = urlopen(requests)
    pagina = bs(html, "html.parser")
    
    return pagina

def upload_raw_zone(bs4: bs, indicador: str):
    '''Salva a página html localmente e faz o upload do arquivo .html para raw-zone'''
    # Dando nome ao arquivo html 
    arquivo_local= datetime.now().strftime('%Y-%m-%d')+f'_{indicador}.html'

    # Escrevendo o html localmente
    with open(arquivo_local, "w") as file:
        file.write(str(bs4))
        
    print(f'Gerando arquivo.html {indicador.upper()}  na raw-zone...')
    wr.s3.upload(
        local_file = arquivo_local,
        path = f's3://XXXXXX0336967/raw-zone/crawlers/pricing/cepea/{indicador}/fat/{arquivo_local}',
        boto3_session = session,
    )
    print(f'Upload do arquivo {arquivo_local} na raw feito com sucesso!')
    
def upload_staging_zone(df, indicador: str):
    '''Faz upload do arquivo csv na staging-zone '''
    
    df.insert(0, "cepea_indicador", indicador, True) # Adicionando a coluna com o nome do indicador
    
    print(F'Gerando .csv {indicador.upper()} na staging-zone...')
    wr.s3.to_csv(
        df = df,
        path = f's3://XXXXXXXX36967/staging-zone/crawlers/pricing/cepea/{indicador}/fat/year={date_today:%Y}/month={date_today:%m}/day={date_today:%d}/',
        dataset = True,
        mode = 'append', 
        index=False,
        boto3_session = session,
        encoding="ISO-8859-15",
        header=True
    )
    print(f'csv {indicador.upper()} gerado com sucesso!!!\n')
    

def milho():
    nome = 'milho'
    pagina = get_pagina(nome)
    upload_raw_zone(pagina, nome)
    table_html = pagina.find('table', {'id': 'imagenet-indicador1'})

    # Extraindo a linha com os dados atuais 
    linha = list(table_html.tbody.tr.get_text().split('\n'))[1:6]
    
    # Passando para dicionário
    dic = {'data': linha[0],
        'valor_real': linha[1],
        'var_dia': linha[2],
        'var_mes': linha[3],
        'valor_dolar': linha[4],
        'ultima_atualizacao': hora_atual()}
    
    df = pd.DataFrame(data=[dic])
    upload_staging_zone(df, nome)
    
def soja():
    nome = 'soja'
    pagina = get_pagina(nome)
    upload_raw_zone(pagina, nome)
    # Soja é o indicador2
    table_html = pagina.find('table', {'id': 'imagenet-indicador2'})
    
    # Extraindo a linha com os dados atuais 
    linha = list(table_html.tbody.tr.get_text().split('\n'))[1:6]
    
    # Passando para dicionário
    dic = {'data': linha[0],
        'valor_real': linha[1],
        'var_dia': linha[2],
        'var_mes': linha[3],
        'valor_dolar': linha[4],
        'ultima_atualizacao': hora_atual()}
    
    df = pd.DataFrame(data=[dic])
    upload_staging_zone(df, nome)



def frango():
    nome = 'frango'
    pagina = get_pagina(nome)
    upload_raw_zone(pagina, nome)
    # Frango não tem dolar por isso é feito aqui 
    table_html = pagina.find('table', {'id': 'imagenet-indicador1'})
    
    linha = list(table_html.tbody.tr.get_text().split('\n'))[1:5]
    
    dic = {'data': linha[0],
        'valor_real': linha[1],
        'var_dia': linha[2],
        'var_mes': linha[3],
        'ultima_atualizacao': hora_atual()}
    
    df = pd.DataFrame(data=[dic])
    upload_staging_zone(df, nome)



def boi_gordo():
    nome = 'boi-gordo'
    pagina = get_pagina(nome)
    upload_raw_zone(pagina, nome)
    table_html = pagina.find('table', {'id': 'imagenet-indicador1'})
    
    # Extraindo a linha com os dados atuais 
    linha = list(table_html.tbody.tr.get_text().split('\n'))[1:6]
    
    # Passando para dicionário
    dic = {'data': linha[0],
        'valor_real': linha[1],
        'var_dia': linha[2],
        'var_mes': linha[3],
        'valor_dolar': linha[4],
        'ultima_atualizacao': hora_atual()}
    
    df = pd.DataFrame(data=[dic])
    upload_staging_zone(df, nome)


def suino():
    nome = 'suino'
    pagina = get_pagina(nome)
    upload_raw_zone(pagina, nome)
    
    table_html = pagina.find('table', {'id': 'imagenet-indicador2'})
    
    linhas = table_html.tbody.get_text().split('\n')
    linhas = linhas[0:35] # pegando apenas as últimas linhas
    linhas = list(filter(None, linhas)) # tirando vazias
    
    df = pd.DataFrame(columns=['data','estado', 'valor_vista', 'var_dia', 'var_mes'])
    
    # Adicionando as linhas de dados ao Dataframe
    while len(linhas) != 0:
        df.loc[len(df)]= [linhas[0], linhas[1], linhas[2], linhas[3], linhas[4]]
        
        del[linhas[0:5]] # não deleta o 5
    
    df['ultima_atualizacao'] = hora_atual()
    upload_staging_zone(df, nome)
    
if __name__ == '__main__':
    try: 
        milho()
    except ValueError: 
        print(ValueError)
        pass  # Caso aconteça um erro vai executar as outras coletas normalmente 
    
    try: 
        soja()
    except ValueError: 
        print(ValueError)
        pass
    
    try: 
        frango()
    except ValueError: 
        print(ValueError)
        pass
    
    try: 
        boi_gordo()
    except ValueError: 
        print(ValueError)
        pass
    
    try: 
        suino()
    except ValueError: 
        print(ValueError)
        pass
    
    print('FIM!!!!!!!')