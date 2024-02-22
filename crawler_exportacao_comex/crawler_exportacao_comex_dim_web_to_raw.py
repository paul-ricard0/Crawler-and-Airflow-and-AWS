from datetime import datetime
from time import time, sleep

import pandas as pd
import os
import io
import ssl
import boto3
import awswrangler as wr
aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']


BUCKET_NAME = "XXXXXX080336967"
session = boto3.Session(
    region_name = "us-east-1",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

ssl._create_default_https_context = ssl._create_unverified_context


hoje = datetime.today().date()

ano  = hoje.year


if datetime.now().month == 1:
    currentYear = datetime.now().year-1
    currentMonth = datetime.now().month+11
else:
    currentYear = datetime.now().year
    currentMonth = datetime.now().month-1

urls = {
    "dim" : ["co_ncm","co_pais","co_bloco","co_isiccuci","co_nbm","co_nbmncm","co_ncmcgce","co_ncmcuci","co_ncmfatagreg","co_ncmisic","co_ncmppe","co_ncmppi","co_ncmsh","co_uf","co_ufmun","co_unid","co_urf","co_via"],
    "url" : ["NCM","PAIS","PAIS_BLOCO","ISIC_CUCI","NBM","NBM_NCM","NCM_CGCE","NCM_CUCI","NCM_FAT_AGREG","NCM_ISIC","NCM_PPE","NCM_PPI","NCM_SH","UF","UF_MUN","NCM_UNIDADE","URF","VIA"]
}

df_dim = pd.DataFrame(urls)
df_dim= df_dim.reset_index()




for row in df_dim.itertuples(index=False):
    #   print(getattr(row, 'url'))
    dim = getattr(row, 'dim')
    url = getattr(row, 'url')
    link = f"https://balanca.economia.gov.br/balanca/bd/tabelas/{url}.csv"
    # print(link)
    df = pd.read_csv(link,delimiter = ';',on_bad_lines='skip',encoding = "ISO-8859-1")
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer)
    csv_buffer.seek(0)
    nome_raw = datetime.now().strftime('%Y%m%d')+f'_DIM_{url}_{currentYear}_{currentMonth}.csv'
    print(nome_raw)
    with io.BytesIO() as buffer:
        df.to_csv(buffer)
        buffer.seek(0)    
        wr.s3.upload(
            local_file=buffer, 
            path = f's3://{BUCKET_NAME}/raw-zone/crawlers/exportacao/comexstat/dim/year={hoje:%Y}/month={hoje:%m}/day={hoje:%d}/{nome_raw}', 
            boto3_session = session
            )
    print(f'Upload do arquivo {nome_raw} gravado com sucesso!')







