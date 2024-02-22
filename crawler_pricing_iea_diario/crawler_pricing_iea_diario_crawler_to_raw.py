from datetime import datetime, date, timedelta
from time import time, sleep

import pandas as pd
import re
import os

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import chromedriver_binary
import requests


import boto3
import awswrangler as wr

# Variáveis de ambiente
aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']


# Criando a sessão do Boto3
session = boto3.Session(
    region_name = "us-east-1",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key)




link = 'http://ciagri.iea.sp.gov.br/BancoDeDados/PrecosDiarios/Atacado'

def hora_atual():
    return datetime.fromtimestamp(time()).strftime('%Y-%m-%d %H:%M:%S')

def click(path: str):
    browser.find_element(By.XPATH, path).click()
    sleep(2)


from pyvirtualdisplay import Display
display = Display(visible=False, size=(1920, 1080))  
print('Iniciando Display')
display.start()
sleep(2)



print(f"CRAWLER INICIADO {hora_atual()}")
chromeOptions = webdriver.ChromeOptions()
chromeOptions.add_argument("--start-maximized") #open Browser in maximized mode
chromeOptions.add_argument("--no-sandbox") #bypass OS security model
chromeOptions.add_argument("--disable-dev-shm-usage") #overcome limited resource problems
prefs = {"download.default_directory" : "/app/Downloads"}
chromeOptions.add_experimental_option("prefs",prefs)
browser=webdriver.Chrome(options = chromeOptions)

sleep(1)
browser.get(link)





print('FILTRANDO PRODUTOS...')
sleep(1)
click('//*[@id="app-produtos"]/div[2]/button') # Filtro Produtos
click('//*[@id="produtos-modal"]/div/div/div[3]/div/div[1]/button[2]') # Limpando filtros
click('//*[@id="produtos-modal"]/div/div/div[2]/div/div[2]/div[8]/label') # Carne suína - Bisteca (carré)
click('//*[@id="produtos-modal"]/div/div/div[2]/div/div[2]/div[9]/label' ) # Carne suína - Copa
click('//*[@id="produtos-modal"]/div/div/div[2]/div/div[2]/div[10]/label' ) # Carne suína - Lombo
click('//*[@id="produtos-modal"]/div/div/div[2]/div/div[2]/div[11]/label' ) # Carne suína - Pernil s/osso
click('//*[@id="produtos-modal"]/div/div/div[2]/div/div[3]/div[2]/label' ) # Frango - Coxa
click('//*[@id="produtos-modal"]/div/div/div[2]/div/div[3]/div[3]/label' ) # Frango - Frango resfriado
click('//*[@id="produtos-modal"]/div/div/div[2]/div/div[3]/div[4]/label' ) # rango - Peito sem osso
click('//*[@id="produtos-modal"]/div/div/div[2]/div/div[3]/div[5]/label' ) # Frango - Sobrecoxa
click('//*[@id="produtos-modal"]/div/div/div[2]/div/div[3]/div[9]/label' ) # Manteiga - Com sal
click('//*[@id="produtos-modal"]/div/div/div[2]/div/div[3]/div[10]/label' ) # Manteiga - Sem sal
click('//*[@id="produtos-modal"]/div/div/div[2]/div/div[3]/div[29]/label' ) # Queijo - Mussarela
click('//*[@id="produtos-modal"]/div/div/div[2]/div/div[3]/div[31]/label' ) # Queijo - Prato
click('//*[@id="produtos-modal"]/div/div/div[2]/div/div[3]/div[34]/label' ) # Salsicha Hot Dog - Salsicha Hot Dog
click('//*[@id="produtos-modal"]/div/div/div[3]/div/div[2]/button[2]')







print('FILTRANDO DATA... ', end='')

ontem = date.today() - timedelta(1)
print(ontem)
browser.find_element(By.XPATH, '//*[@id="start_date"]').clear()
sleep(2)
browser.find_element(By.XPATH, '//*[@id="start_date"]').send_keys(ontem.strftime('%d/%m/%Y'))
sleep(2)
click('//*[@id="submit-data"]') # Enviando data
    




sleep(5)
print('COLETANDO DADOS...')

html = browser.find_element(By.XPATH, '//*[@id="resultados"]').get_attribute('innerHTML')
soup = BeautifulSoup(html)    



sleep(2)

if len(str(soup).split()) < 50: 
    
    print(">>>>>>>>>> NÃO EXISTE DADOS PARA SER COLETADOS <<<<<<<<< ")

else:
    resultado = []
    print("CRIANDO DATAFRAME...")
    for linha in soup.find_all('tr')[1:]:
        dado = linha.find_all('td')
        coluna0 = dado[0]
        coluna1 = dado[1]
        coluna2 = dado[2]
        coluna3 = dado[3]
        coluna4 = dado[4]
        coluna5 = dado[5]
        coluna6 = dado[6]
        coluna7 = dado[7]
        coluna8 = dado[8]
        resultado.append({'origem': coluna0.text,
                            'produto': coluna1.text,
                            'descricao': coluna2.text,
                            'unidade': coluna3.text,
                            'data': coluna4.text,   
                            'preco_min': coluna5.text,
                            'preco_med': coluna6.text,
                            'preco_max': coluna7.text,
                            'mercado': coluna8.text,})
        
    df = pd.DataFrame(resultado)
    
    # Inserindo a coluna com a data do dia do PDF e ultima atualizacao
    df=df.assign(ultima_atualizacao=hora_atual())
    
    id_lista = []

    for index, linha in df.iterrows():
        cod = linha['descricao'] + linha['unidade'] + linha['data'] 
        cod = cod.lower()
        cod = re.sub(r"[^a-zA-Z0-9]","",cod)
        id_lista.append(cod)
        
        
    df['id_coleta'] = id_lista  
    
    
    try:
        wr.s3.to_csv(
            df = df,
            path = 's3://a3data-548080336967/raw-zone/crawlers/pricing/iea_diario/fat/',
            dataset = True,
            mode = 'append',
            boto3_session = session,
            encoding="ISO-8859-15",
            index=False,
            header=True
        )
        
        print("CSV criado na RAW-ZONE com SUCESSO!")
    except:
        print('ERRO AO SUBIR CSV PARA O S3!!!!')
        
        
        
        
browser.close()
sleep(1)

# Crawler  executado com sucesso!
print(f'Dados IEA atualizados com sucesso as: {hora_atual()}!')
