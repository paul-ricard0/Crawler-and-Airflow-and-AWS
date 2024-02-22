from airflow.decorators import dag
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

from datetime import datetime
from airflow.models import Variable

aws_access_key_id = Variable.get('aws_access_key_id')
aws_secret_access_key = Variable.get('aws_secret_access_key')

BUCKET_SCRIPTS = 'x-x'

# Default settings applied to all tasks
default_args = {
    "owner": "Paulo Ricardo Lima",
    "depends_on_past": False,
    "start_date": datetime(2022, 2, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1
}

#“At 09:00 on every day-of-week from Monday through Friday.” 
@dag(
    default_args=default_args,
    dag_id="crawler_pricing_cepea",
    schedule_interval="0 9 * * 1-5", # Execução programada para todos os dias as 9hrs (5hrs horário de brasília)
    catchup=False,
    tags=["pricing", "crawler"],
    description="Pipeline coleta os dados do iea do dia 01/01/2022 até a data atual.",
)

# Bloco que será executado como container 
def data_pipeline_cepea():
    '''	Pipeline que utiliza os dados do ibge para carregar tabela no S3 e Athena.'''

    task_cepea_api_to_raw_to_staging = KubernetesPodOperator(    
        task_id = 'executa_cepea_api_to_raw_to_staging',
        namespace = 'airflow',
        image = 'xxxxxxxxxxxxxxxxxxxxxxx',
        arguments =  ['/app/crawler_cepea_to_raw_to_staging.py'],
        name = 'execute_cepea_to_raw_to_staging',
        env_vars = {'AWS_ACCESS_KEY_ID': aws_access_key_id,
                    'AWS_SECRET_ACCESS_KEY': aws_secret_access_key},
        is_delete_operator_pod = True,
        in_cluster = True,
        get_logs = True,
        image_pull_policy='Always'
    )
    
    task_cepea_staging_to_consumer = KubernetesPodOperator(    
        task_id = 'executa_cepea_staging_to_consumer',
        namespace = 'airflow',
        image = 'xxxxxxxxxxxxxxxxxxxxxxxxxxx',
        arguments =  ['/app/crawler_cepea_staging_to_consumer.py'],
        name = 'executa_cepea_staging_to_consumer',
        env_vars = {'AWS_ACCESS_KEY_ID': aws_access_key_id,
                    'AWS_SECRET_ACCESS_KEY': aws_secret_access_key},
        is_delete_operator_pod = True,
        in_cluster = True,
        get_logs = True,
        image_pull_policy='Always'
    )
        
    # control flow
    task_cepea_api_to_raw_to_staging >> task_cepea_staging_to_consumer

execucao=data_pipeline_cepea()           
