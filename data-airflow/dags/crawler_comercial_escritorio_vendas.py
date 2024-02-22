from airflow.decorators import dag
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

from datetime import datetime, timedelta
from airflow.models import Variable

AWS_ACCESS_KEY_ID = Variable.get('aws_access_key_id')
AWS_SECRET_ACCESS_KEY = Variable.get('aws_secret_access_key')

GED_USER = Variable.get('GED_USER')
GED_PASSWORD = Variable.get('GED_PASSWORD')


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

# Executa todo dia 1 do mês mas tem que executar manualmente quando tiver uma alteração 
@dag(
    default_args=default_args,
    dag_id="crawler_comercial_escritorio_vendas",
    schedule_interval='0 0 1 * *',  # Todo o dia 1 do mês
    catchup=False,
    tags=["x", "crawler"],
    description="x"
)

# Bloco que será executado como container 
def data_pipeline_escritorio_vendas():

    task_escritorio_vendas_crawler_to_consumer = KubernetesPodOperator(    
        task_id = 'executa_escritorio_vendas_crawler_to_consumer',
        namespace = 'airflow',
        image = 'xxxxxxxxxxxxxxx',
        arguments =  ['/app/escritorio_vendas.py'],
        name = 'execute_escritorio_vendas_crawler_to_consumer',
        env_vars = {'AWS_ACCESS_KEY_ID': AWS_ACCESS_KEY_ID,
                    'AWS_SECRET_ACCESS_KEY': AWS_SECRET_ACCESS_KEY,
                    'GED_USER': GED_USER,
                    'GED_PASSWORD': GED_PASSWORD},
        is_delete_operator_pod = True,
        in_cluster = True,
        get_logs = True,
        image_pull_policy='Always'
    )
    
  
    # control flow
    task_escritorio_vendas_crawler_to_consumer 

execucao=data_pipeline_escritorio_vendas()           
