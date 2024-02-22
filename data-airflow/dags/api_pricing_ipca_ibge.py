from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

import time
import boto3
from datetime import datetime, timedelta
from airflow.models import Variable

aws_access_key_id = Variable.get('aws_access_key_id')
aws_secret_access_key = Variable.get('aws_secret_access_key')

BUCKET_SCRIPTS = 'xxxxx-xxxxx'

# Default settings applied to all tasks
default_args = {
    'owner': 'Paulo Ricardo Lima',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email': ['x@x.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

@dag(
    default_args=default_args,
    dag_id='api_pricing_ipca_ibge',
    schedule_interval='0 6 13 * *', # Execução programada para todos os dias 13 as 6 horas
    catchup=False,
    tags=['api', 'x'],
    description='xxxx ',
)
# Bloco que será executado como container 
def data_pipeline_ipca_ibge():
    '''	Pipeline que utiliza os dados do ibge para carregar tabela no S3 e Athena.'''
    
    task_ipcaibge_api_to_raw = KubernetesPodOperator(    
        task_id = 'task_ipcaibge_api_to_raw',
        namespace = 'airflow',
        image = 'x',
        arguments =  ['/app/ipcaibge_api_to_raw.py'],
        name = 'task_ipcaibge_api_to_raw',
        env_vars = {'AWS_ACCESS_KEY_ID': aws_access_key_id,
                    'AWS_SECRET_ACCESS_KEY': aws_secret_access_key},
        is_delete_operator_pod = True,
        in_cluster = True,
        get_logs = True,
        image_pull_policy='Always'
    )

    task_ipcaibge_raw_to_staging = KubernetesPodOperator(    
        task_id = 'task_ipcaibge_raw_to_staging',
        namespace = 'airflow',
        image = 'x',
        arguments =  ['/app/ipcaibge_raw_to_staging.py'],
        name = 'task_ipcaibge_raw_to_staginge',
        env_vars = {'AWS_ACCESS_KEY_ID': aws_access_key_id,
                    'AWS_SECRET_ACCESS_KEY': aws_secret_access_key},
        is_delete_operator_pod = True,
        in_cluster = True,
        get_logs = True,
        image_pull_policy='Always'
    )

    
    task_ipcaibge_staging_to_consumer = KubernetesPodOperator(    
        task_id = 'task_ipcaibge_staging_to_consumer',
        namespace = 'airflow',
        image = 'x',
        arguments =  ['/app/ipcaibge_staging_to_consumer.py'],
        name = 'task_ipcaibge_staging_to_consumer',
        env_vars = {'AWS_ACCESS_KEY_ID': aws_access_key_id,
                    'AWS_SECRET_ACCESS_KEY': aws_secret_access_key},
        is_delete_operator_pod = True,
        in_cluster = True,
        get_logs = True,
        image_pull_policy='Always'
    )
    
    # control flow
    task_ipcaibge_api_to_raw >> task_ipcaibge_raw_to_staging >> task_ipcaibge_staging_to_consumer


execucao=data_pipeline_ipca_ibge()           
