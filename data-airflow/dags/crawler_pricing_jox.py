from airflow.decorators import dag
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

from datetime import datetime, timedelta
from airflow.models import Variable

aws_access_key_id = Variable.get('aws_access_key_id')
aws_secret_access_key = Variable.get('aws_secret_access_key')

IMAGE = 'x'
BUCKET_SCRIPTS = 'x-x'

# Default settings applied to all tasks
default_args = {
    'owner': 'Paulo Ricardo Lima',
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 2),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

# Execução programada Às 22:00 em todos os dias da semana, de segunda a sábado.
@dag(
    dag_id = 'crawler_pricing_jox',
    default_args = default_args,
    schedule_interval = "00 22 * * 1-5",  
    catchup=False, # para não executar todas as dags desde o inicio quando subir uma nova
    tags=['crawler','pricing'],
    description="Pipeline coleta os dados do JOX_FRANGO, JOX_FRANGO_VIVO e JOX_SUINO. Os dados são coletados de um arquivo PDF(raw-zone), convertidos em CSV (Staging-zone) e transformados em parquet(consumer-zone) e disponibilizados em formato de tabela pelo através do athena",
) 


# Bloco que será executado como container 
def data_pipeline_jox():
    '''	Pipeline que utiliza os dados do ibge para carregar tabela no S3 e Athena.'''

    task_jox_crawler_to_raw = KubernetesPodOperator(    
        task_id = 'executa_jox_crawler_to_raw',
        namespace = 'airflow',
        image = IMAGE,
        arguments =  ['/app/jox_crawler_to_raw.py'],
        name = 'execute_jox_crawler_to_raw',
        env_vars = {'AWS_ACCESS_KEY_ID': aws_access_key_id,
                    'AWS_SECRET_ACCESS_KEY': aws_secret_access_key},
        is_delete_operator_pod = True,
        in_cluster = True,
        get_logs = True,
        image_pull_policy='Always'
    )
    
    task_jox_raw_to_staging = KubernetesPodOperator(    
        task_id = 'executa_jox_raw_to_staging',
        namespace = 'airflow',
        image = IMAGE,
        arguments =  ['/app/jox_raw_to_staging.py'],
        name = 'executa_jox_raw_to_staging',
        env_vars = {'AWS_ACCESS_KEY_ID': aws_access_key_id,
                    'AWS_SECRET_ACCESS_KEY': aws_secret_access_key},
        is_delete_operator_pod = True,
        in_cluster = True,
        get_logs = True,
        image_pull_policy='Always'
    )
    
    task_jox_staging_to_consumer = KubernetesPodOperator(    
        task_id = 'executa_jox_staging_to_consumer',
        namespace = 'airflow',
        image = IMAGE,
        arguments =  ['/app/jox_staging_to_consumer.py'],
        name = 'executa_jox_staging_to_consumer',
        env_vars = {'AWS_ACCESS_KEY_ID': aws_access_key_id,
                    'AWS_SECRET_ACCESS_KEY': aws_secret_access_key},
        is_delete_operator_pod = True,
        in_cluster = True,
        get_logs = True,
        image_pull_policy='Always'
    )
        
    # control flow
    task_jox_crawler_to_raw >> task_jox_raw_to_staging >> task_jox_staging_to_consumer

execucao=data_pipeline_jox()           
