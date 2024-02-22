from datetime import datetime
import os, io

from bs4 import BeautifulSoup
import pandas as pd

import boto3

# Variáveis de ambiente
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
BUCKET_NAME = 'XXXXXXXX548080336967'

# Criando a client do Boto3
s3_client = boto3.client(
    's3', 
    region_name='us-east-1',
    aws_access_key_id=AWS_ACCESS_KEY_ID, 
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY 
)

date = datetime.now()
def hora_atual() -> str:
    """Retorna a hora atual no formato '%Y-%m-%d %H:%M:%S'"""
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def extrarir_tabela(soup: BeautifulSoup, columns: list) -> pd.DataFrame:
    """Extrai tabela de HTML e retorna como um Pandas DataFrame."""
    print('Extraindo tabela do html... ', hora_atual())
    
    div_data = soup.find('div', {'class': 'frase_cotacao_hoje'}) 
    div_data = div_data.get_text()
    lista_data = div_data.split('-')
    data_cotacao = lista_data[1].strip()
    
    tb  = soup.find('div', {'id': 'ajuste_altura'})
    tb = tb.ul.get_text()
    tb = tb.replace('R$', '')
    # pasando para lista
    tb_lista = tb.split('\n')  
    # removendo todos os items '' da lista e passando para uma nova lista
    tb_lista_limpa = [elemento for elemento in tb_lista if elemento != ''] 
    
    df = pd.DataFrame(columns=columns)
    for linha in tb_lista_limpa:
        linha = linha.split(':')
        linha[1] = linha[1].strip()
        df.loc[len(df)] = linha
    df.insert(2, "data_cotacao", data_cotacao, True)
    df.insert(3, "ultima_atualizacao", hora_atual(), True) # inserindo coluna ultima_atualizacao
    
    return df

def upload_to_staging(df: pd, key_parquet: str) -> None:
    """ 
    Transforma o dataframe em parquet 
    e fazer upload do arquivo para a staging-zone
    """
    
    print('Criando espaço na memória... ', hora_atual())
    parquet_buffer = io.BytesIO() 
    print('Salvando na memória em formato parquet... ', hora_atual())
    df.to_parquet(parquet_buffer, index=True) 
    
    print('Subindo parquet para a staging-zone... ', hora_atual())
    s3_client.put_object(
        Body=parquet_buffer.getvalue(),
        Bucket=BUCKET_NAME, 
        Key=key_parquet 
    )
    print('Upload finalizado com sucesso!!!', hora_atual(), '\n')
    
if __name__ == '__main__':
    indicadores = {'graos': ['grao', 'preco'],
                   'suinos_base': ['item', 'preco'],
                   'suinos_bolsa': ['estado', 'preco']}
    
    for indicador in indicadores:
        print(indicador.upper(), hora_atual())
        df_columns = indicadores[indicador]
        
        key_html_raw = f'raw-zone/crawlers/pricing/acss/{indicador}/{date.strftime("%Yy-%mm")}/acss_{indicador}_{date.strftime("%Y-%m-%d")}.html' 
        key_parquet_staging = f'staging-zone/crawlers/pricing/acss/{indicador}/fat/year={date:%Y}/month={date:%m}/day={date:%d}/acss_{indicador}_{date.strftime("%Y-%m-%d")}.parquet'
        arquivo_local = f'{indicador}_{date.strftime("%Y-%m-%d")}.html'
        
        print('Fazendo download do arquivo na raw-zone... ', hora_atual())
        s3_client.download_file(BUCKET_NAME, key_html_raw, arquivo_local)
        
        print('Lendo html... ', hora_atual())
        with open(arquivo_local, "r") as arquivo:
            conteudo = arquivo.read()
            soup = BeautifulSoup(conteudo, 'html.parser')
            
        df = extrarir_tabela(soup, df_columns)
        
        upload_to_staging(df, key_parquet_staging)