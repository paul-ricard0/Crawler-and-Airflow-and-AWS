import time
from datetime import datetime
import os.path
import os
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from shareplum import Office365
from shareplum import Site
from shareplum.site import Version
import boto3

from time import sleep

hoje = datetime.today().date()

CAMINHO_DOWNLOAD = "/app/Downloads"
ARQUIVO_SAIDA = "Todososprodutos-ROTA.csv"

# BUCKET NAME DELTA TABLES
BUCKET_NAME = 'CCCCCCCCCCCCCC336967'
KEY_RAW = f'raw-zone/crawlers/scanntech/mensal/rota/year={hoje:%Y}/month={hoje:%m}/day={hoje:%d}/{ARQUIVO_SAIDA}'

EMAIL = 'XXXXXXXXXXXXXXXXXXXX'
PW = 'PXXXXXXXXXXXXXXXXXX'

office_login = 'https://ssso.scanntech.com/aut/a=2381f94e-7dae-4ea8-8105-f3fa2f93edfe&response_mode=fragment&response_type=code&scope=openid&nonce=0411bf79-93c7-4877-8d6b-b76b4c38fc5e'
forms_resposta = 'https://analytics.7/dashboard'
forms_dash = 'https://tableau4.sgin=viz_share_link#1'

'''CONFIGURAÇÕES CHROME DRIVE'''
from pyvirtualdisplay import Display
display = Display(visible=False, size=(1920, 1080))  
print('Iniciando Display')
display.start()
sleep(2)

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
browser.get(office_login)
browser.maximize_window()

bi = browser.find_element(By.XPATH,'//*[@id="username"]')
bi.send_keys(EMAIL)
time.sleep(20)
bi_pass = browser.find_element(By.XPATH,'//*[@id="password"]')
bi_pass.send_keys(PW)
bi_pass.send_keys(Keys.ENTER)

browser.get(forms_resposta)
time.sleep(20)
browser.get(forms_dash)
time.sleep(20)

download = browser.find_element(By.XPATH,'//*[@id="download-ToolbarButton"]/span[1]')
download.click()
time.sleep(20) 

download_arquivo = browser.find_element(By.XPATH,'//*[@id="DownloadDialog-Dialog-Body-Id"]/div/fieldset/button[3]')
download_arquivo.click() 
time.sleep(20)

selecionar_csv = browser.find_element(By.CSS_SELECTOR,"#export-crosstab-options-dialog-Dialog-BodyWrapper-Dialog-Body-Id > div > div.foyjxgp > div:nth-child(2) > div > label:nth-child(2)").click()
time.sleep(20)

baixa_arquivo = browser.find_element(By.CSS_SELECTOR,"#export-crosstab-options-dialog-Dialog-BodyWrapper-Dialog-Body-Id > div > div.fdr6v0d > button").click()
time.sleep(60)


#FECHAR O BROWSER
browser.close()
browser.quit()

lista_arquivos = os.listdir(CAMINHO_DOWNLOAD)

lista_data = []

for arquivo in lista_arquivos:
    if ".csv" in arquivo:
        data = os.path.getctime(f"{CAMINHO_DOWNLOAD}/{arquivo}")
        lista_data.append((data,arquivo))
        
lista_data.sort(reverse=True)
ultimo_arquivo = lista_data[0]

#print(ultimo_arquivo[1])
arquivo_origem = ultimo_arquivo[1]

caminho_arquivo = os.path.join(f"{CAMINHO_DOWNLOAD}", f"{arquivo_origem}")
   
authcookie = Office365('https://XXXXXXXXXXXXX.sharepoint.com/', 
                       username='gXXXXXXXXXXXXXXXXXX', 
                       password='#XXXXXXXXXXXXXXXXXXXXXXX1@').GetCookies()

site = Site('https://XXXXXXXXXXXXXXXX.sharepoint.com/sites/XXXXXXXXXXXXXXXXXX/', 
            version=Version.v365, 
            authcookie=authcookie);

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
os.remove(caminho_arquivo)

