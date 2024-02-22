from airflow.decorators import dag
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

from datetime import datetime
from airflow.models import Variable

aws_access_key_id = Variable.get('aws_access_key_id')
aws_secret_access_key = Variable.get('aws_secret_access_key')

BUCKET_SCRIPTS = 'X-X'

# Default settings applied to all tasks
default_args = {
    "owner": "Paulo Ricardo Lima",
    "depends_on_past": False,
    "start_date": datetime(2023, 2, 3),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1
}

#“At 16:30 on every day-of-week from Tuesday through Saturday.”
# a página atualiza os das as 10 horas da manhã
@dag(
    default_args=default_args,
    dag_id="crawler_pricing_scot",
    schedule_interval="30 16 * * 2-6", # Execução programada para Terça a Sábado as 16:30hrs (12:30hrs horário de brasília)
    catchup=False,
    tags=["pricing", "crawler"],
    description="Coleta de dados do consultoria Scot de Terça a Sábado",
)

# Bloco que será executado como container 
def data_pipeline_scot():
    '''	Pipeline que utiliza os dados do ibge para carregar tabela no S3 e Athena.'''

    task_scot_crawler_to_raw = KubernetesPodOperator(    
        task_id = 'executa_scot_crawler_to_raw',
        namespace = 'airflow',
        image = 'X',
        arguments =  ['/app/scot_crawler_to_raw.py'],
        name = 'execute_scot_crawler_to_raw',
        env_vars = {'AWS_ACCESS_KEY_ID': aws_access_key_id,
                    'AWS_SECRET_ACCESS_KEY': aws_secret_access_key},
        is_delete_operator_pod = True,
        in_cluster = True,
        get_logs = True,
        image_pull_policy='Always'
    )
    
    task_scot_raw_to_staging = KubernetesPodOperator(    
        task_id = 'executa_scot_raw_to_staging',
        namespace = 'airflow',
        image = 'X',
        arguments =  ['/app/scot_raw_to_staging.py'],
        name = 'executa_scot_raw_to_staging',
        env_vars = {'AWS_ACCESS_KEY_ID': aws_access_key_id,
                    'AWS_SECRET_ACCESS_KEY': aws_secret_access_key},
        is_delete_operator_pod = True,
        in_cluster = True,
        get_logs = True,
        image_pull_policy='Always'
    )
    
    task_scot_staging_to_consumer = KubernetesPodOperator(    
        task_id = 'executa_scot_staging_to_consumer',
        namespace = 'airflow',
        image = 'X',
        arguments =  ['/app/scot_staging_to_consumer.py'],
        name = 'executa_scot_staging_to_consumer',
        env_vars = {'AWS_ACCESS_KEY_ID': aws_access_key_id,
                    'AWS_SECRET_ACCESS_KEY': aws_secret_access_key},
        is_delete_operator_pod = True,
        in_cluster = True,
        get_logs = True,
        image_pull_policy='Always'
    )
        
    # control flow
    task_scot_crawler_to_raw >> task_scot_raw_to_staging >> task_scot_staging_to_consumer

execucao=data_pipeline_scot()           
