print('Iniciando tarefa de evolução de arquivos da pipeline crawler-exportacao-comex-stat da raw-zone para staging-zone.')
import boto3
import pandas as pd
import os
import io
from datetime import datetime
import time

hoje =  datetime.today().date()

hora_atual = datetime.today()
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
# AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")


BUCKET_NAME = "XXXXXX080336967"

if datetime.now().month == 1:
    currentYear = datetime.now().year-1
    currentMonth = datetime.now().month+11
else:
    currentYear = datetime.now().year
    currentMonth = datetime.now().month-1



# coletar o ultimo arquivo modificado 
s3 = boto3.client('s3')

print('buscando caminho do arquivo na raw-zone...')


urls = {
    "dim" : ["co_ncm","co_pais","co_bloco","co_isiccuci","co_nbm","co_nbmncm","co_ncmcgce","co_ncmcuci","co_ncmfatagreg","co_ncmisic","co_ncmppe","co_ncmppi","co_ncmsh","co_uf","co_ufmun","co_unid","co_urf","co_via"],
    "url" : ["NCM","PAIS","PAIS_BLOCO","ISIC_CUCI","NBM","NBM_NCM","NCM_CGCE","NCM_CUCI","NCM_FAT_AGREG","NCM_ISIC","NCM_PPE","NCM_PPI","NCM_SH","UF","UF_MUN","NCM_UNIDADE","URF","VIA"]
}

df_dim = pd.DataFrame(urls)
df_dim= df_dim.reset_index()


for row in df_dim.itertuples(index=False):
    dim = getattr(row, 'dim')
    url = getattr(row, 'url')
    nome_raw = datetime.now().strftime('%Y%m%d')+f'_DIM_{url}_{currentYear}_{currentMonth}.csv'
    print('lendo o arquivo para um DataFrame')
    df = pd.read_csv(
        f"s3://{BUCKET_NAME}/raw-zone/crawlers/exportacao/comexstat/dim/year={hoje:%Y}/month={hoje:%m}/day={hoje:%d}/{nome_raw}",
        storage_options={
            "key": AWS_ACCESS_KEY_ID,
            "secret": AWS_SECRET_ACCESS_KEY
            # "token": AWS_SESSION_TOKEN,
        },
        sep=","   
    
    )
    print('tratando nomes de colunas do DataFrame...')
    normalized_columns = df.columns.str.lower().str.strip()
    df.columns = normalized_columns

    df.drop(columns=['unnamed: 0'], inplace = True)
    df['ultima_atualizacao'] = hora_atual
    # print(df)
    print('salvando DataFrame em parquet no buffer...')
    out_buffer = io.BytesIO()
    df.to_parquet(out_buffer, index=False)

    print('salvando parquet na staging-zone...')
    s3.put_object(Bucket=BUCKET_NAME, Key=f'staging-zone/crawlers/exportacao/comexstat/dim/comex_stat_dim_{url}.parquet', Body=out_buffer.getvalue())


    print('Tarefa de evolução de comexstat da raw-zone para staging-zone finalizada.')
