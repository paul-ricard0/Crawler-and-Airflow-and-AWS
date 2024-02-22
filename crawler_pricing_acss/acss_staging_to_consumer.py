from datetime import datetime
import os

import boto3

# Credenciais aws
BUCKET_NAME = 'XXXXXXXX8080336967'
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY'] 

date = datetime.now()
def hora_atual() -> str:
    """Retorna a hora atual no formato '%Y-%m-%d %H:%M:%S'"""
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

print('Criando a client do Boto3', hora_atual())
s3_client=boto3.client(
    's3', 
    region_name='us-east-1',
    aws_access_key_id=AWS_ACCESS_KEY_ID, 
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY 
)

if __name__ == '__main__':
    indicadores = ['graos', 'suinos_base', 'suinos_bolsa']
    
    for indicador in indicadores:
        print(indicador.upper(), hora_atual())
        key_parquet_staging = f'staging-zone/crawlers/pricing/acss/{indicador}/fat/year={date:%Y}/month={date:%m}/day={date:%d}/acss_{indicador}_{date.strftime("%Y-%m-%d")}.parquet'
        key_parquet_consumer = f'consumer-zone/crawlers/pricing/acss/{indicador}/fat/year={date:%Y}/month={date:%m}/day={date:%d}/acss_{indicador}_{date.strftime("%Y-%m-%d")}.parquet'
        key_manifest = f'consumer-zone/crawlers/pricing/acss/{indicador}/fat/_symlink_format_manifest/manifest'
        
        print('Copiando parquet da staging para consumer...', hora_atual())
        response = s3_client.copy_object(
            Bucket=BUCKET_NAME,
            CopySource=BUCKET_NAME+'/'+key_parquet_staging,
            Key=key_parquet_consumer
        ) 
        
        print('Baixando manifest link...', hora_atual())
        manifest_obj = s3_client.get_object(
            Bucket=BUCKET_NAME,
            Key=key_manifest
        )

        print('Decodificando manifest...', hora_atual())
        manifest_contents = manifest_obj['Body'].read( ).decode('utf-8')

        print('Adicionando nova linha no manifest...', hora_atual())
        manifest_contents_new = f'{manifest_contents}s3://{BUCKET_NAME}/{key_parquet_consumer}\n'

        print('Sobreescrevendo manifest na CONSUMER...', hora_atual())
        s3_client.put_object(
            Body=manifest_contents_new,
            Bucket=BUCKET_NAME,
            Key=key_manifest,
        )
        print('Finalizado com sucesso!!!', hora_atual())
                