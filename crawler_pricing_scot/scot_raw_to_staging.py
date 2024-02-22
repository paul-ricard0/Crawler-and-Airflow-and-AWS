import requests
from bs4 import BeautifulSoup as bs

from datetime import datetime
from time import time
import os
import pandas as pd

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

# Variável para retornar o dia de hoje
date_today = datetime.today().date()

def hora_atual(): # Função pra retornar a hora atual
    return datetime.fromtimestamp(time()).strftime('%Y-%m-%d %H:%M:%S')
                                                                                                            
def get_data(soup: bs): 
    data = soup.find("div", class_="noticias_table_coluna2" ).text

    # Substitui uma substring por alguma outra coisa.
    data = data.replace("Segunda-feira,", "")
    data = data.replace("Terça-feira,", "")
    data = data.replace("Quarta-feira,", "")
    data = data.replace("Quinta-feira,", "")
    data = data.replace("Sexta-feira,", "")
    data = data.replace("Sábado,", "")
    data = data.replace("Domingo,", "")
    data = data.replace("- 18h00", "")

    # Acertando os Meses
    data = data.replace("de janeiro de", "-01-")
    data = data.replace("de fevereiro de", "-02-")
    data = data.replace("de março de", "-03-")
    data = data.replace("de abril de", "-04-")
    data = data.replace("de maio de", "-05-")
    data = data.replace("de junho de", "-06-")
    data = data.replace("de julho de", "-07-")
    data = data.replace("de agosto de", "-08-")
    data = data.replace("de setembro de", "-09-")
    data = data.replace("de outubro de", "-10-")
    data = data.replace("de novembro de", "-11-")
    data = data.replace("de dezembro de", "-12-")

    # Acertando espaços
    data = data.replace(" ", "")
    data = data.replace("\n", "")

    data = "0" + data # Adicionando o dígito zero para os dias 1 ao  9
    data = data[0:11] # Selecionando apenas a data
    data = data[-10:] # Retirando o 0 que fica a mais a partir do dia 10
    
    print('printando o data:', data)
    return data

def upload_staging_zone(df, indicador: str):
    '''Faz upload do arquivo csv na staging-zone '''
    
    df['ultima_atualizacao'] = hora_atual() # adicionando coluna hora da coleta  
    
    print(F'Gerando .csv SCOT_{indicador.upper()} na staging-zone...')
    wr.s3.to_csv(
        df = df,
        path = f's3://CCCCCCCCCCCC80336967/staging-zone/crawlers/pricing/scot/{indicador}/fat/year={date_today:%Y}/month={date_today:%m}/day={date_today:%d}/',
        dataset = True,
        mode = 'append', 
        index = False,
        boto3_session = session,
        encoding="ISO-8859-15",
        header=True
    )
    
    print(f'Cotação Scot - Consultoria {indicador.upper()} atualizada com sucesso as: {hora_atual()}!\n')
    
def cotacao_atacado():
    nome = 'atacado'

    # request código html
    res = requests.get(URL+f'{nome}/')
    html_doc = res.text

    # estrutura de dados aninhada
    soup = bs(html_doc, 'html.parser')

    df = pd.read_html(html_doc,    encoding='UTF-8')
    df = df[0]
    df.rename(columns={df.columns[0]: 'atacado', df.columns[1]: 'preco_boi', df.columns[3]: 'preco_ha1ano'}, inplace=True) #arrumando o nome das colunas
    df['preco_vaca'] = '' # adicionando a coluna preco_vaca


    df2 = df[['atacado', 'preco_boi', 'preco_vaca', 'preco_ha1ano']] # passando só as colunas que quero 


    df_atacado = pd.DataFrame(columns=df2.columns)


    for linha in df2.itertuples():  
        
        if linha.preco_ha1ano.isdigit(): # ESTOU VALIDANDO SE É UMA LINHA QUE QUERO PEGAR 
            
            p_ano = float(linha.preco_ha1ano) / 100 # transformando em númerico 
            p_ano = "%.2f" % p_ano
            p_ano = p_ano.replace('.', ',')
            
            
            if linha.atacado=='Vaca casada' or linha.atacado=='Novilha casada': # se for vaca
                p_vaca = float(linha.preco_boi) / 100
                p_vaca = "%.2f" % p_vaca
                p_vaca = p_vaca.replace('.', ',')
            
                df_atacado.loc[len(df_atacado)] = [linha.atacado, '-', p_vaca, p_ano]
            else: # se for boi
                p_boi = float(linha.preco_boi) / 100
                p_boi = "%.2f" % p_boi
                p_boi = p_boi.replace('.', ',')
                
                df_atacado.loc[len(df_atacado)] = [linha.atacado, p_boi, '-', p_ano] 

    data = get_data(soup)

    df_atacado['data'] = data.replace(" ", "") # Adicionando coluna da data

    upload_staging_zone(df=df_atacado, indicador=nome)
    return df_atacado

def cotacao_boi_gordo():
    nome = 'boi-gordo'
    # request código html
    res = requests.get(URL+f'{nome}/')
    html_doc = res.text
    
    # estrutura de dados aninhada
    soup = bs(html_doc, 'html.parser')
    
    df = pd.read_html(html_doc, encoding='UTF-8')
    df = df[1]
    df.rename(columns={df.columns[0]: 'local', df.columns[1]: 'a_vista_funrural', df.columns[3]: 'trinta_dias_funrural', 
                       df.columns[5]: 'base',  df.columns[6]: 'a_vista', df.columns[8]: 'trinta_dias'}, inplace=True) #arrumando o nome das colunas
    
    df2 = df[['local', 'a_vista_funrural', 'trinta_dias_funrural', 'base', 'a_vista', 'trinta_dias']]
    
    df_boi = pd.DataFrame(columns=df2.columns)
    
    for linha in df2.itertuples(): 
        if linha.a_vista.isdigit(): # ESTOU VALIDANDO SE É UMA LINHA QUE QUERO PEGAR 
            
            av_fun = float(linha.a_vista_funrural) / 100 # transformando em númerico 
            av_fun = "%.2f" % av_fun
            av_fun = av_fun.replace('.', ',')
            
            d30_fun = float(linha.trinta_dias_funrural) / 100
            d30_fun = "%.2f" % d30_fun
            d30_fun = d30_fun.replace('.', ',')
            
            av  = float(linha.a_vista) / 100
            av = "%.2f" % av
            av = av.replace('.', ',')
            
            d30 = float(linha.trinta_dias) / 100
            d30 = "%.2f" % d30
            d30 = d30.replace('.', ',')

            df_boi.loc[len(df_boi)] = [linha.local, av_fun, d30_fun, linha.base, av, d30]     
    
    data = get_data(soup)
    
    df_boi['data'] = data.replace(" ", "") # Adicionando coluna da data

    df_boi.rename(columns={'trinta_dias_funrural': '30_dias_funrural', 'trinta_dias': '30_dias'}, inplace=True)
        
    upload_staging_zone(df=df_boi, indicador=nome)   
    return df_boi

if __name__ == '__main__':
    try: 
        cotacao_atacado()
    except ValueError: 
        print('ERRO ATACADO!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
        print(ValueError)
        pass
    
    try: 
        cotacao_boi_gordo()
    except ValueError: 
        print('ERRO BOI-GORDO!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
        print(ValueError)
        pass
  
    print('\nCOLETA FINALIZADA!!!!!!!')
