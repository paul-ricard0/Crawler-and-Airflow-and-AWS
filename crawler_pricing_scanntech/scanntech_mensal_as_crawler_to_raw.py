from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import chromedriver_binary

from datetime import datetime
from time import sleep
import os.path
import os

from shareplum import Office365
from shareplum import Site
from shareplum.site import Version

import boto3

hoje = datetime.today().date()

CAMINHO_DOWNLOAD = "/app/Downloads"
ARQUIVO_SAIDA = "Todososprodutos-AS.csv"

# BUCKET NAME DELTA TABLES
BUCKET_NAME = 'XXXXXXXXXXX36967'
KEY_RAW = f'raw-zone/crawlers/scanntech/mensal/as/year={hoje:%Y}/month={hoje:%m}/day={hoje:%d}/{ARQUIVO_SAIDA}'

EMAIL = 'XXXXXXXXXX'
PW = 'XXXXXXXXXXXXXX'


office_login = 'https://ssso.scanntech.copenid-co4sponse_type=code&scope=openid&nonce=0411bf79-93c7-4877-8d6b-b76b4c38fc5e'
forms_resposta = 'https://analytics.05337/dashboard'
forms_dash = 'https://tableau4cing-MensaleSetoMarca/825c/Mensal-AS?%3Aembed=y&%3Adisplay_count=n&%3AshowVizHome=n&%3Aorigin=viz_share_link#1'

'''CONFIGURAÇÕES CHROME DRIVE'''
from pyvirtualdisplay import Display
display = Display(visible=False, size=(1920, 1080))  
print('Iniciando Display')
display.start()
sleep(2)
print('Display inicializado.')

chromeOptions = webdriver.ChromeOptions()
print('Webdriver ChromeOptions.')
chromeOptions.add_argument("--start-maximized") #open Browser in maximized mode
chromeOptions.add_argument("--no-sandbox") #bypass OS security model
chromeOptions.add_argument("--disable-gpu") #bypass OS security model
chromeOptions.add_argument("--allow-running-insecure-content")
chromeOptions.add_argument("--disable-dev-shm-usage")
chromeOptions.add_argument("start-maximized")
prefs = {"download.default_directory" : "/app/Downloads"}
chromeOptions.add_experimental_option("prefs",prefs)
print('Webdriver add_experimental_option.')

browser = webdriver.Chrome(options = chromeOptions)
print('Browser iniciado.')
print('Acessando página de login do Office...')
browser.get(office_login)
browser.maximize_window()

print('Fazendo login do Office...')
bi = browser.find_element(By.XPATH,'//*[@id="username"]')
bi.send_keys(EMAIL)
sleep(20)
bi_pass = browser.find_element(By.XPATH,'//*[@id="password"]')
bi_pass.send_keys(PW)
bi_pass.send_keys(Keys.ENTER)

print("Acessar forms_resposta")
browser.get(forms_resposta)
sleep(20)
print("Acessar forms_dash...")
response=browser.get(forms_dash)
sleep(20)
print('Página acessada. Response:')
print(response)

print("Clicar no botão baixar...")
download = browser.find_element(By.XPATH,'/html/body/div[2]/div[2]/div[2]/div[1]/div[2]/div[3]/span[1]')
download.click()
sleep(20) 

print("Clicar para baixar tabela de referência cruzada...")
download_arquivo = browser.find_element(By.XPATH,'//*[@id="DownloadDialog-Dialog-Body-Id"]/div/fieldset/button[3]')
download_arquivo.click() 
sleep(20)

print("Clicar para selecionar formato csv...")
selecionar_csv = browser.find_element(By.CSS_SELECTOR,"#export-crosstab-options-dialog-Dialog-BodyWrapper-Dialog-Body-Id > div > div.foyjxgp > div:nth-child(2) > div > label:nth-child(2)").click()
sleep(20)

print("Clicar para baixar arquivo...")
baixa_arquivo = browser.find_element(By.CSS_SELECTOR,"#export-crosstab-options-dialog-Dialog-BodyWrapper-Dialog-Body-Id > div > div.fdr6v0d > button").click()
sleep(60)

#FECHAR O BROWSER
browser.close()
browser.quit()

lista_arquivos = os.listdir(CAMINHO_DOWNLOAD)
print(lista_arquivos)
lista_data = []

for arquivo in lista_arquivos:
    if ".csv" in arquivo:
        data = os.path.getctime(f"{CAMINHO_DOWNLOAD}/{arquivo}")
        lista_data.append((data,arquivo))
        
lista_data.sort(reverse=True)

ultimo_arquivo = lista_data[0]

print(ultimo_arquivo[1])
arquivo_origem = ultimo_arquivo[1]

old_file = os.path.join(f"{CAMINHO_DOWNLOAD}", f"{arquivo_origem}")
new_file = os.path.join(f"{CAMINHO_DOWNLOAD}", ARQUIVO_SAIDA)
os.rename(old_file, new_file)

caminho_arquivo = os.path.join(f"{CAMINHO_DOWNLOAD}",ARQUIVO_SAIDA)

authcookie = Office365('https://XXXX.sharepoint.com/', 
                       username='XXXXXXXXXXXXXXX.com.br', 
                       password='XXXXXXXXXXXX1@').GetCookies()

site = Site('https://XXXXXXXXXXXXXX.sharepoint.com/sites/XXXXXXXXXXXXXXXX/', 
            version=Version.v365, 
            authcookie=authcookie)

# Pasta de destino
folder = site.Folder('Documentos%20Partilhados/Database/Scanntech')

# Leitura do arquivo preenchendo direto no 
with open(caminho_arquivo, mode='rb') as file:
           fileContent = file.read()
           
print ('Jogando o csv para a pasta de destino' , folder)
folder.upload_file(fileContent, ARQUIVO_SAIDA)
print ('Arquivo carregado ')

# Upload the file
s3 = boto3.client('s3')
response = s3.upload_file(caminho_arquivo, BUCKET_NAME, KEY_RAW)

### Removendo na origem
os.remove(new_file)

