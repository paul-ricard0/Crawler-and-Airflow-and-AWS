from datetime import datetime
from time import sleep
import os, io

import boto3
import awswrangler as wr

# Credenciais aws
aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']

s3_client = boto3.client(
    's3', 
    region_name='us-east-1',
    aws_access_key_id=aws_access_key_id, 
    aws_secret_access_key=aws_secret_access_key 
)

BUCKET_NAME = 'XXXXXXXXXXXXXX0336967'
# Variável para retornar o dia de hoje
date_today = datetime.today().date()

if __name__ == '__main__':
    
    indicadores = ['frango_vivo', 'frango', 'suino'] 
    
    for indicador in indicadores:
        
        print('\n\n'+ indicador.upper())
        
        path_staging = f'staging-zone/crawlers/pricing/jox/{indicador}/fat/year={date_today:%Y}/month={date_today:%m}/day={date_today:%d}/'
        path_parquet = f'consumer-zone/crawlers/pricing/jox/{indicador}/fat/year={date_today:%Y}/month={date_today:%m}/day={date_today:%d}/jox_{indicador}_{date_today}.parquet'
        key_manifest = f'consumer-zone/crawlers/pricing/jox/{indicador}/fat/_symlink_format_manifest/manifest'
    
        # lista arquivos na pasta e pega a chave (caminho) do último arquivo
        print('Lendo último arquivo na staging...')
        ultimo_arquivo = s3_client.list_objects(
            Bucket=BUCKET_NAME,
            Prefix= path_staging
        )['Contents'][-1]['Key']
        sleep(2)
        # lendo o arquivo csv para formato de dataframe 
        print('Criando dataframe...')
        df = wr.s3.read_csv(f's3://{BUCKET_NAME}/{ultimo_arquivo}', encoding="ISO-8859-15")
        df = df.astype(str)
        sleep(5)
        print('Criando parquet...')
        out_buffer = io.BytesIO() # criando espaço na memória
        df.to_parquet(out_buffer, index=False) # salvando na memória em formato parquet
        sleep(2)
        print('Subindo parquet para a consumer-zone...')
        s3_client.put_object(Bucket=BUCKET_NAME, Key=path_parquet, Body=out_buffer.getvalue())
        sleep(5)
        print('Baixando manifest link...')
        manifest_obj = s3_client.get_object(
            Bucket=BUCKET_NAME,
            Key=key_manifest
        )
        
        print('Decodificando manifest...')
        manifest_contents = manifest_obj['Body'].read( ).decode('utf-8')
        
        print('Adicionando nova linha no manifest...')
        manifest_contents_new = f'{manifest_contents}s3://{BUCKET_NAME}/{path_parquet}\n'
        sleep(2)
        print('Sobreescrevendo manifest na CONSUMER...')
        s3_client.put_object(
            Body=manifest_contents_new,
            Bucket=BUCKET_NAME,
            Key=key_manifest,
        )
        print('Finalizado com sucesso!!!')
