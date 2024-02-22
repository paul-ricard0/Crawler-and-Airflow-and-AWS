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



BUCKET_NAME = "XXXXXXX48080336967"

if datetime.now().month == 1:
    currentYear = datetime.now().year-1
    currentMonth = datetime.now().month+11
else:
    currentYear = datetime.now().year
    currentMonth = datetime.now().month-1

s3_client=boto3.client(
    's3', 
    region_name='us-east-1',
    aws_access_key_id=AWS_ACCESS_KEY_ID, 
    aws_secret_access_key= AWS_SECRET_ACCESS_KEY
)


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
    print('Copiando objeto da staging-zone para consumer-zone...')
    print(f'consumer-zone/crawlers/exportacao/comexstat/dim/comex_stat_dim_{url}.parquet')
    response = s3_client.copy_object(
        Bucket=BUCKET_NAME,
        CopySource=BUCKET_NAME+'/'+f'staging-zone/crawlers/exportacao/comexstat/dim/comex_stat_dim_{url}.parquet',
        Key=f'consumer-zone/crawlers/exportacao/comexstat/dim/comex_stat_dim_{url}.parquet',
    )


    print('Apendando caminho do novo arquivo de dados ao conteúdo do Manifest...')

    manifest_contents = 's3://' +BUCKET_NAME+'/'+f'consumer-zone/crawlers/exportacao/comexstat/dim/comex_stat_dim_{url}.parquet'


    print('Colocando novo Manifest no S3...')
    response_manifest = s3_client.put_object(
        Body=manifest_contents,
        Bucket=BUCKET_NAME,
        Key=f'consumer-zone/crawlers/exportacao/comexstat/dim/_symlink_format_manifest/{url}/manifest',
    )

    print('Tarefa de evolução de comexstat da staging-zone para consumer-zone finalizada.')