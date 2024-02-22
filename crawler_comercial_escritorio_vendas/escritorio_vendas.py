from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File 

from shareplum import Office365
from shareplum import Site
from shareplum.site import Version

from datetime import datetime
import pandas as pd 
import os, io

import boto3

# Obtendo a data atual
data_atual = datetime.now()

print('Autenticação')
site_url = "https://XXXXXX.sharepoint.com/sites/XXXXXXXXXX/"
username =  os.environ['GED_USER'] 
password = os.environ['GED_PASSWORD']
authcookie = Office365('https://XXXXXXXX.sharepoint.com/', username, password).GetCookies()

print('Acessando site')
site = Site(site_url, version=Version.v365, authcookie=authcookie)

print('Acessando pasta com os documentos')
folder = site.Folder('Documentos%20Partilhados/1 - Database/')
df_foler = pd.DataFrame(folder.files)

print('Pegando a url do MD_GERAL.xlsx')
df_foler = df_foler.filter(items=['LinkingUri', 'Name']) # filtrando apenas as duas colunas
df_foler = df_foler.loc[df_foler['Name'] == 'MD_GERAL.xlsx'] # filtrando apenas MD_GERAL.xlsx
df_foler.index = range(len(df_foler.index)) # passando index novamente
url = df_foler.iloc[0,0]

ctx_auth = AuthenticationContext(url)
if ctx_auth.acquire_token_for_user(username, password):
    ctx = ClientContext(url, ctx_auth)
    web = ctx.web
    ctx.load(web)
    ctx.execute_query()
response = File.open_binary(ctx, url)

bytes_file_obj = io.BytesIO() #save data to BytesIO stream
bytes_file_obj.write(response.content)
bytes_file_obj.seek(0) #set file object to start

print('read excel file and each sheet into pandas dataframe')
df = pd.read_excel(bytes_file_obj,sheet_name='Unid Venda')

df = df.drop(df.columns[21:27], axis=1) # deletando a parte de equipe venda
df = df.drop(df.columns[25:35], axis=1) 
df = df.dropna(how='all')

def get_dim( df: pd.DataFrame, index01: int, index02: int) -> pd.DataFrame:
    dim = df.iloc[:, index01:index02]
    dim = dim.dropna(how='all') # tirando todas as linahs completamente vazias
    dim.columns = dim.iloc[0] # Define a primeira linha como cabeçalho
    dim = dim.drop(df.index[0])
    return dim

dim_empresa = get_dim(df,0,2)
dim_mercado = get_dim(df,3,5)
dim_unidade_geral_venda = get_dim(df,6,8)
dim_escritorio_venda = get_dim(df,13,20)
dim_escritorio_regional = get_dim(df,22,25)
dim_filial_venda = get_dim(df,9,12)
dim_filial_venda = dim_filial_venda.rename(columns={'Unid Geral ID': 'Unid ID'})

df_fat = pd.merge(dim_escritorio_venda, dim_mercado, on='Mercado ID', how='left')
df_fat = pd.merge(df_fat, dim_empresa, on='Empresa ID', how='left')
df_fat = pd.merge(df_fat, dim_escritorio_regional, on='Escritorio ID', how='left')
dim_filial_venda = pd.merge(dim_filial_venda, dim_unidade_geral_venda, on='Unid ID', how='left')
df_fat = pd.merge(df_fat, dim_filial_venda, on='Filial ID', how='left')

df_final = df_fat.iloc[:, [0, 1, 2, 7, 3, 8, 4, 11, 9, 9, 10, 10]]
columns_name = ['nk_escritorio_venda', 'escritroio_venda', 'nk_mercado', 'mercado', 'nk_empresa', 'empresa', 'nk_filial_venda', 'filial_venda', 'nk_regional_venda', 'regional_venda', 'nk_canal_venda', 'canal_venda']
df_final.columns = columns_name
df_final.insert(0, "data", data_atual.strftime('%Y-%m-01'), True)
df_final['ultima_atualizacao'] = data_atual.strftime('%Y-%m-%d %H:%M:%S')


# Credenciais aws
BUCKET_NAME = 'XXXXXXXX580336967'
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY'] 
print('S3_client')
s3_client=boto3.client(
    's3', 
    region_name='us-east-1',
    aws_access_key_id=AWS_ACCESS_KEY_ID, 
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY 
)

key_parquet_consumer = f'consumer-zone/crawlers/comercial/escritorio_venda/fat/year={data_atual.strftime("%Y")}/month={data_atual.strftime("%m")}/escritorio_venda_{data_atual.strftime("%Y-%m")}.parquet'
key_manifest = f'consumer-zone/crawlers/comercial/escritorio_venda/fat/_symlink_format_manifest/manifest{data_atual.strftime("%Y-%m")}'

print('buffer')
out_buffer = io.BytesIO() # criando espaço na memória
df_final.to_parquet(out_buffer, index=False) # salvando na memória em formato parquet
s3_client.put_object(Bucket=BUCKET_NAME, Key=key_parquet_consumer, Body=out_buffer.getvalue())

manifest_contents_new = f's3://{BUCKET_NAME}/{key_parquet_consumer}\n'

print('manifest')
s3_client.put_object(
    Body=manifest_contents_new,
    Bucket=BUCKET_NAME,
    Key=key_manifest,
)

print('FINALIZADO COM SUCESSO!!!!')