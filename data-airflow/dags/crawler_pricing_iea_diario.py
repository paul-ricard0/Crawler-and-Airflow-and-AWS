from airflow.decorators import dag
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

from datetime import datetime
from airflow.models import Variable

aws_access_key_id = Variable.get("aws_access_key_id")
aws_secret_access_key = Variable.get("aws_secret_access_key")

IMAGE = 'x'
# Default settings applied to all tasks
default_args = {
    "owner": "Paulo Ricardo Lima",
    "depends_on_past": False,
    "start_date": datetime(2022, 1, 25),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1
}


@dag(
    default_args=default_args,
    dag_id="crawler_pricing_iea_diario",
    schedule_interval="0 10 * * *", # Execução programada para todos os dias as 10hrs (7hrs horário de brasília)
    catchup=False,
    tags=["pricing", "crawler"],
    description="Pipeline coleta os dados do iea do dia 01/01/2022 até a data atual.",
)

# Bloco que será executado como container 
def data_pipeline_iea_diario():
    """
    
    Pipeline que utiliza os dados do iea diario para carregar tabela no Athena.
    """
    
    task_iea_diario_crawler_to_raw = KubernetesPodOperator(    
        task_id = "task_iea_diario_crawler_to_raw",
        namespace = 'airflow',
        image = IMAGE,
        arguments = ['/app/crawler_pricing_iea_diario_crawler_to_raw.py'],
        name = 'task_iea_diario_crawler_to_raw',
        env_vars = {'AWS_ACCESS_KEY_ID': aws_access_key_id,
                    'AWS_SECRET_ACCESS_KEY': aws_secret_access_key},
        is_delete_operator_pod = True,
        in_cluster = True,
        get_logs = True,
        image_pull_policy='Always'
    )

    
    task_iea_diario_raw_to_staging = KubernetesPodOperator(    
        task_id = "task_iea_diario_raw_to_staging",
        namespace = 'airflow',
        image = IMAGE,
        arguments = ['/app/crawler_pricing_ieadiario_raw_to_staging.py'],
        name = 'task_iea_diario_raw_to_staging',
        env_vars = {'AWS_ACCESS_KEY_ID': aws_access_key_id,
                    'AWS_SECRET_ACCESS_KEY': aws_secret_access_key},
        is_delete_operator_pod = True,
        in_cluster = True,
        get_logs = True,
        image_pull_policy='Always'
    )    

    task_iea_diario_staging_to_consumer = KubernetesPodOperator(    
        task_id = "task_iea_diario_staging_to_consumer",
        namespace = 'airflow',
        image = IMAGE,
        arguments = ['/app/crawler_pricing_ieadiario_staging_to_consumer.py'],
        name = 'task_iea_diario_staging_to_consumer',
        env_vars = {'AWS_ACCESS_KEY_ID': aws_access_key_id,
                    'AWS_SECRET_ACCESS_KEY': aws_secret_access_key},
        is_delete_operator_pod = True,
        in_cluster = True,
        get_logs = True,
        image_pull_policy='Always'
    )
    # Control Flow
    task_iea_diario_crawler_to_raw >> task_iea_diario_raw_to_staging >> task_iea_diario_staging_to_consumer

execucao = data_pipeline_iea_diario()           
