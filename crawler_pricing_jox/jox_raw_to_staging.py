import pandas as pd
import camelot
import tabula 
import requests
import warnings

from datetime import datetime 
from time import time, sleep
import os

import awswrangler as wr
import boto3

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
nome_pdf = datetime.fromtimestamp(time()).strftime('%Y-%m-%d-%Hh-%Mm')+'.pdf'
date_today = datetime.today().date()

# Função pra retornar a hora atual
def hora_atual():
    return datetime.fromtimestamp(time()).strftime('%Y-%m-%d %H:%M:%S')

# Função que checa a data
def validate_date(date_text):
    try:
        datetime.strptime(date_text, "%d.%m.%Y")
        return True
    except ValueError:
        return False
    
# Função que faz upload do arquivo para a staging-zone
def upload_to_staging(data_frame, indicador):
    data_frame = data_frame.astype(str) # passando as colunas para o tipo str
    print(f'Escrevendo o csv JOX-{indicador.upper()} na Staging S3...')
    wr.s3.to_csv(
        df = data_frame,
        path = f's3://XXXXXXXXXXXXX336967/staging-zone/crawlers/pricing/jox/{indicador}/fat/year={date_today:%Y}/month={date_today:%m}/day={date_today:%d}/',
        dataset = True,
        mode = 'append', 
        index=False,
        encoding="ISO-8859-15",
        boto3_session = SESSION,
    )
    print(f'Upload CSV cotação do JOX-{indicador.upper()} feita com sucesso: {hora_atual()}!')
    
def jox_frango_vivo():
    print('\n\nIniciando JOX-FRANGO-VIVO!!!!!')
    print('Criando dataframe através do pdf...')
    df_frango_vivo = tabula.read_pdf(nome_pdf, 
                            pages='2',
                            encoding="ISO-8859-15",
                            area=(22,18, 130 ,310),
                            pandas_options={'header': None})[0]
    
    print('Dropando tudo que for vazio pegando o exemplo do resfriado...')
    df_frango_vivo.drop([1,3], axis='columns', inplace=True)
    df_frango_vivo.columns = ['frango_vivo', 'fr_vivo_reais']
    
    df_frango_vivo['frv_codinome'] =''
    df_frango_vivo = df_frango_vivo.dropna(subset=["fr_vivo_reais"]) #limpar primeira leva de dados
    df_frango_vivo = df_frango_vivo.dropna(subset=["frango_vivo"]) #limpar segunda leva de dados
    
    # Se não for dia de Segunda
    if dia_da_semana != 0:  
        sp = df_frango_vivo.set_index(df_frango_vivo['frango_vivo']).filter(like='Granja Interior São Paulo', axis=0)
        df_frango_vivo_sp = sp.assign(frv_codinome = ['Frango Vivo SP'])
        mg = df_frango_vivo.set_index(df_frango_vivo['frango_vivo']).filter(like='Minas Gerais (Avimig) Frango Vivo Fob', axis=0)
        df_frango_vivo_mg = mg.assign(frv_codinome = ['Frango Vivo MG'])
        fr_vivo_final = pd.concat([df_frango_vivo_sp, df_frango_vivo_mg])

    # Se for dia de Segunda
    elif dia_da_semana == 0:
        sp = df_frango_vivo.set_index(df_frango_vivo['frango_vivo']).filter(like='Granja Interior São Paulo', axis=0)
        df_frango_vivo_sp = sp.assign(frv_codinome = ['Frango Vivo SP','Frango Vivo SP'])
        mg = df_frango_vivo.set_index(df_frango_vivo['frango_vivo']).filter(like='Minas Gerais (Avimig) Frango Vivo Fob', axis=0)
        df_frango_vivo_mg = mg.assign(frv_codinome = ['Frango Vivo MG'])
        fr_vivo_final = pd.concat([df_frango_vivo_sp, df_frango_vivo_mg])
    else:
        print('Erro no tamanho da tabela Frango Vivo, favor verificar o dia da semana!')
        
    print('Resetando para insert...')
    fr_vivo_final = fr_vivo_final.reset_index(level=0, drop=True).reset_index()
    
    print('Reindex das colunas...')
    fr_vivo_final = fr_vivo_final[['frango_vivo', 'frv_codinome', 'fr_vivo_reais']]
    
    print('Inserindo a coluna com a data do dia do PDF e ultima atualizacao...')
    fr_vivo_final.insert(0, 'data', data, allow_duplicates=False)
    fr_vivo_final['ultima_atualizacao'] = hora_atual()
    
    upload_to_staging(data_frame=fr_vivo_final, indicador='frango_vivo')    
    
def jox_frango():
    print('\n\nIniciando JOX-FRANGO!!!!!')
    print('lendo tabela no pdf...')
    df_frango = tabula.read_pdf(nome_pdf, 
                            pages='2',
                            encoding="ISO-8859-15",
                            area=(32,18 , 210 , 700),
                            pandas_options={'header': None},
                            stream=True,
                            guess=False)[0]
    
    print('Dropando coluna por semama...')
    try:   
        # Se for dia de Segunda
        if dia_da_semana == 0 and data == '15.08.2022':
            df_frango.drop([0,1,2,3,5,6], axis='columns', inplace=True)  
            
        elif dia_da_semana != 0 and data == '26.08.2022' :
            print('com data zuada')
            df_frango.drop([0,3,5], axis='columns', inplace=True)   

        elif dia_da_semana != 0 and data == '30.11.2022' :
            print('com data zuada')
            df_frango.drop([0,1,2,5,7], axis='columns', inplace=True)

        elif dia_da_semana != 0:# Se não for segunda
            df_frango.drop([0,1,2,3,6,8], axis='columns', inplace=True) 

        elif dia_da_semana == 0:# Se for dia de Segunda
            df_frango.drop([0,1,2,3,6,8], axis='columns', inplace=True)      

    except:      
        print('Erro ao dropar colunas por semana')   
        
    df_frango.columns = ['frango_abatido', 'resfriado_reais', 'congelado_reais']
    
    print('Dropando tudo que for vazio...')
    df_frango = df_frango.dropna(subset=["resfriado_reais"])
    df_frango = df_frango.dropna(subset=["frango_abatido"])

    print('Replace corrigindo a coluna frango_abatido_sp...') 
    df_frango[['frango_abatido_sp']] = df_frango['frango_abatido'].str.replace('Kg','').str.split('/', n=1, expand=True)
    
    try:
        # Se for dia de Segunda
        if dia_da_semana == 0 and data == '15.08.2022':
            df_frango[['congelado_reais']] = df_frango['congelado_reais'].str.replace(' Cal','').str.replace(' Fir','').str.replace(' Fra','').str.replace('Fra','').str.split( n=1, expand=True)

        elif dia_da_semana != 0:
            df_frango[['congelado_reais']] = df_frango['congelado_reais'].str.replace(' Cal','').str.replace(' Fir','').str.replace(' Fra','').str.replace('Fra','').str.split( n=1, expand=True) 

        elif dia_da_semana == 0:
            df_frango[['congelado_reais']] = df_frango['congelado_reais'].str.replace(' Cal','').str.replace(' Fir','').str.replace(' Fra','').str.replace('Fra','').str.split( n=1, expand=True)

    except:
        print('Erro na conversão dos dados congelado_reais')
        
    print('Criando colunas resfriado_min e resfriado_max...')
    #removendo barras duplas quando possuir e criando coluna de média
    df_frango[['resfriado_min', 'resfriado_max']] = df_frango['resfriado_reais'].str.replace('//','/').str.replace(',','.').str.replace('Cal','').str.split('/', n=1, expand=True)

    df_frango[['congelado_reais']] = df_frango['congelado_reais'].str.replace('Cal','').str.split(n=1, expand=True)
    
    print('Convertendo para float para fazer a média...')
    df_frango['resfriado_min'] = pd.to_numeric(df_frango['resfriado_min'],errors = 'coerce')
    df_frango['resfriado_max'] = pd.to_numeric(df_frango['resfriado_max'],errors = 'coerce')
    df_frango['media_resfriado'] = df_frango[['resfriado_min', 'resfriado_max']].mean(axis=1).round(2)
    
    print('Criando colunas congelado_min congelado_max...')
    df_frango[['congelado_min', 'congelado_max']] = df_frango['congelado_reais'].str.replace('//','/').str.replace(',','.').str.replace('Cal','').str.split('/', n=1, expand=True)
    df_frango['congelado_min'] = pd.to_numeric(df_frango['congelado_min'],errors = 'coerce')
    df_frango['congelado_max'] = pd.to_numeric(df_frango['congelado_max'],errors = 'coerce')
    df_frango['media_congelado'] = df_frango[['congelado_min', 'congelado_max']].mean(axis=1).round(2)
    
    print('Dropando coluna frango_abatido...')
    df_frango.drop(['frango_abatido'], axis='columns', inplace=True)

    print('Fazendo reindex das colunas')
    df_frango = df_frango.reindex(columns=['frango_abatido_sp'
                                        ,'resfriado_reais'
                                        ,'congelado_reais'
                                        ,'resfriado_min'
                                        ,'resfriado_max'
                                        ,'media_resfriado'
                                        ,'congelado_min'
                                        ,'congelado_max'
                                        ,'media_congelado'])
    
    print('Inserindo a coluna com a data do dia do PDF e ultima atualizacao...')
    df_frango.insert(0, 'data', data, allow_duplicates=False)
    df_frango['ultima_atualizacao'] = hora_atual()  
    
    upload_to_staging(data_frame=df_frango, indicador='frango')

def jox_suino():
    print('\n\nIniciando JOX-SUINO!!!!!')
    print('Read a PDF File e gerando JOX-SUINO...')
    df_suino = tabula.read_pdf(nome_pdf, 
                            pages='2',
                            area=(240,20 , 300 , 304),
                            encoding="ISO-8859-15",
                            pandas_options={'header': None},
                            stream=True,
                            guess=False)[0]
    
    print('Dropando colunas e alterando nome...') 
    df_suino.drop([1,3], axis='columns', inplace=True)
    df_suino.columns = ['suino_jox', 'suino_jox_reais']

    print('Dropando tudo que for vazio...')
    df_suino = df_suino.dropna(subset=["suino_jox_reais"])
    
    print('Criando colunas novas com valores separados e')
    print('removendo barras duplas quando possuir e criando coluna de média...')
    df_suino[['suino_min', 'suino_max']] = df_suino['suino_jox_reais'].str.replace('//','/').str.replace(',','.').str.replace('Cal','').str.split('/', n=1, expand=True)
    
    print('Convertendo para float para fazer a média...')
    df_suino['suino_min'] = pd.to_numeric(df_suino['suino_min'],errors = 'coerce')
    df_suino['suino_max'] = pd.to_numeric(df_suino['suino_max'],errors = 'coerce')
    print('Criando a média...')
    df_suino['media_suino'] = df_suino[['suino_min', 'suino_max']].mean(axis=1).round(2)
    
    print('Inserindo a coluna com a data do dia do PDF e ultima atualizacao...')
    df_suino.insert(0, 'data', data, allow_duplicates=False)
    df_suino['ultima_atualizacao'] = hora_atual()
    
    print('Dropando valores vazios...')
    df_suino = df_suino.dropna(subset=["suino_min"])
    df_suino = df_suino.dropna(subset=["suino_max"])
    
    print('Limpando terminado que não precisa entrar no ambiente para trazer...')
    df_suino.drop(df_suino.index[df_suino['suino_jox'] == 'Terminado Cif Frigorífico São Paulo'], inplace=True)
    df_suino.drop(df_suino.index[df_suino['suino_jox'] == 'Terminado Fob Granja Interior São Paulo'], inplace=True)
    
    upload_to_staging(data_frame=df_suino, indicador='suino')

if __name__ == '__main__':
    
    print('\n'+hora_atual())
    print('Requisitando o boletim diario...')
    response = requests.get(URL)

    print('Salvando o arquivo localmente...') 
    with open(nome_pdf, 'wb') as f:
        f.write(response.content)
    
    print('Desativando os Warnings do Camelot...')
    warnings.filterwarnings("ignore")

    print('Extraindo a Data do PDF...')
    table_data = camelot.read_pdf(nome_pdf,flavor='stream', strip_text='\n', table_areas=['60,770,520,400'])

    data = table_data[0].df[0:2]
    data['data'] = data.apply(''.join, axis=1)
    data = data['data'].iloc[1]
    
    print('Validando  a data do PDF...')
    if validate_date(data): 
        print(f"Data validada: {data}")
    else:
        print('Tentando pela segunda vez...')
        table_data = camelot.read_pdf(URL,flavor='stream', strip_text='\n')
        df_data = table_data[1].df[0:1]
        df_data['data'] = df_data.apply(''.join, axis=1)
        data = df_data['data'].iloc[0]

        if validate_date(data):
            print(f"Data validada na segunda tentativa: {data}")
        else:
            print(f">>>>>>>>>>>>>>>> Segunda tentativa de checagem de data, favor verificar, Data inválida: {data} <<<<<<<<<<<<<<<<<")
            raise Exception('ERRO AO CAPTURAR DATA')
    
    print('Gerando dia da semana...')
    dia_da_semana = data
    dia_da_semana = dia_da_semana.replace(".", "/")
    dia_da_semana = datetime.strptime(dia_da_semana, '%d/%m/%Y').date()
    dia_da_semana = dia_da_semana.weekday()
    print('Dia da Semana:', dia_da_semana)    
    
    sleep(5)
    try: 
        jox_frango_vivo()
    except ValueError: 
        print('ERRO FRANGO_VIVO!!!!!!!!!!!!!!!!!!!!!')
        print(ValueError)
        pass
    
    sleep(5)
    try: 
        jox_frango()
    except ValueError: 
        print('ERRO FRANGO!!!!!!!!!!!!!!!!!!!!!!!!!!')
        print(ValueError)
        pass
    
    sleep(5)
    try: 
        jox_suino()
    except ValueError: 
        print('ERRO SUINO!!!!!!!!!!!!!!!!!!!!!!!!!!!')
        print(ValueError)
        pass
    
    print('\nCOLETA FINALIZADA!!!!!!!')
