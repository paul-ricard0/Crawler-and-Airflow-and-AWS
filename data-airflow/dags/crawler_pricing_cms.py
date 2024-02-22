from airflow.decorators import dag
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

from datetime import datetime, timedelta
from airflow.models import Variable

AWS_ACCESS_KEY_ID = Variable.get('aws_access_key_id')
AWS_SECRET_ACCESS_KEY = Variable.get('aws_secret_access_key')
IMAGE = ''
# Default settings applied to all tasks
default_args = {
    "owner": "Paulo Ricardo Lima",
    "depends_on_past": False,
    "start_date": datetime(2022, 1, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1
}

# Execução programada para todos os dias as 18hs
@dag(
    default_args=default_args,
    dag_id="crawler_pricing_cms",
    schedule_interval="0 10 10 * *",
    catchup=False,
    tags=["crawler", "x"],
    description="Pipeline coleta os dados do C.M.S com base no histórico mensal do PDF, gera a tabela consumer_zone.cms",
)

# Bloco que será executado como container 
def data_pipeline_cms():
    '''	Pipeline que utiliza os dados do ibge para carregar tabela no S3 e Athena.'''

    task_cms_crawler_to_raw = KubernetesPodOperator(    
        task_id = 'executa_cms_crawler_to_raw',
        namespace = 'airflow',
        image = IMAGE,
        arguments =  ['/app/cms_crawler_to_raw.py'],
        name = 'execute_cms_crawler_to_raw',
        env_vars = {'AWS_ACCESS_KEY_ID': AWS_ACCESS_KEY_ID,
                    'AWS_SECRET_ACCESS_KEY': AWS_SECRET_ACCESS_KEY},
        is_delete_operator_pod = True,
        in_cluster = True,
        get_logs = True,
        image_pull_policy='Always'
    )
    
    task_cms_raw_to_staging = KubernetesPodOperator(    
        task_id = 'executa_cms_raw_to_staging',
        namespace = 'airflow',
        image = IMAGE,
        arguments =  ['/app/cms_raw_to_staging.py'],
        name = 'executa_cms_raw_to_staging',
        env_vars = {'AWS_ACCESS_KEY_ID': AWS_ACCESS_KEY_ID,
                    'AWS_SECRET_ACCESS_KEY': AWS_SECRET_ACCESS_KEY},
        is_delete_operator_pod = True,
        in_cluster = True,
        get_logs = True,
        image_pull_policy='Always'
    )
    
    task_cms_staging_to_consumer = KubernetesPodOperator(    
        task_id = 'executa_cms_staging_to_consumer',
        namespace = 'airflow',
        image = IMAGE,
        arguments =  ['/app/cms_staging_to_consumer.py'],
        name = 'executa_cms_staging_to_consumer',
        env_vars = {'AWS_ACCESS_KEY_ID': AWS_ACCESS_KEY_ID,
                    'AWS_SECRET_ACCESS_KEY': AWS_SECRET_ACCESS_KEY},
        is_delete_operator_pod = True,
        in_cluster = True,
        get_logs = True,
        image_pull_policy='Always'
    )
        
    # control flow
    task_cms_crawler_to_raw >> task_cms_raw_to_staging >> task_cms_staging_to_consumer

execucao=data_pipeline_cms()           
