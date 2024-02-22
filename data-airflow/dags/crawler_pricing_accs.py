from airflow.decorators import dag
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

from datetime import datetime, timedelta
from airflow.models import Variable

AWS_ACCESS_KEY_ID = Variable.get('aws_access_key_id')
AWS_SECRET_ACCESS_KEY = Variable.get('aws_secret_access_key')
IMAGE = 'X'
# Default settings applied to all tasks
default_args = {
    "owner": "Paulo Ricardo Lima",
    "depends_on_past": False,
    "start_date": datetime(2021, 7, 30),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1
}

# Execução programada para todos os dias 
@dag(
    default_args=default_args,
    dag_id="crawler_pricing_acss",
    schedule_interval="0 9 * * 1-5",  # “At 09:00 on every day-of-week from Monday through Friday.”
    catchup=False,
    tags=["crawler", "x"],
    description="Pipeline coleta os dados do ACCS disponibilizados no site diariamente dos indicadores de graos suinos_base e suinos_bolsa",
)



# Bloco que será executado como container 
def data_pipeline_acss():

    task_acss_crawler_to_raw = KubernetesPodOperator(    
        task_id = 'executa_acss_crawler_to_raw',
        namespace = 'airflow',
        image = IMAGE,
        arguments =  ['/app/acss_crawler_to_raw.py'],
        name = 'execute_acss_crawler_to_raw',
        env_vars = {'AWS_ACCESS_KEY_ID': AWS_ACCESS_KEY_ID,
                    'AWS_SECRET_ACCESS_KEY': AWS_SECRET_ACCESS_KEY},
        is_delete_operator_pod = True,
        in_cluster = True,
        get_logs = True,
        image_pull_policy='Always'
    )
    
    task_acss_raw_to_staging = KubernetesPodOperator(    
        task_id = 'executa_acss_raw_to_staging',
        namespace = 'airflow',
        image = IMAGE,
        arguments =  ['/app/acss_raw_to_staging.py'],
        name = 'executa_acss_raw_to_staging',
        env_vars = {'AWS_ACCESS_KEY_ID': AWS_ACCESS_KEY_ID,
                    'AWS_SECRET_ACCESS_KEY': AWS_SECRET_ACCESS_KEY},
        is_delete_operator_pod = True,
        in_cluster = True,
        get_logs = True,
        image_pull_policy='Always'
    )
    
    task_acss_staging_to_consumer = KubernetesPodOperator(    
        task_id = 'executa_acss_staging_to_consumer',
        namespace = 'airflow',
        image = IMAGE,
        arguments =  ['/app/acss_staging_to_consumer.py'],
        name = 'executa_acss_staging_to_consumer',
        env_vars = {'AWS_ACCESS_KEY_ID': AWS_ACCESS_KEY_ID,
                    'AWS_SECRET_ACCESS_KEY': AWS_SECRET_ACCESS_KEY},
        is_delete_operator_pod = True,
        in_cluster = True,
        get_logs = True,
        image_pull_policy='Always'
    )
        
    # control flow
    task_acss_crawler_to_raw >> task_acss_raw_to_staging >> task_acss_staging_to_consumer

execucao=data_pipeline_acss()           
