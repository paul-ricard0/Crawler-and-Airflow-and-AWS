from airflow.decorators import dag
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

from datetime import datetime, timedelta
from airflow.models import Variable

aws_access_key_id = Variable.get('aws_access_key_id')
aws_secret_access_key = Variable.get('aws_secret_access_key')

BUCKET_SCRIPTS = 'emr-548080336967'

default_args = {
    "owner": "PAULO ",
    "depends_on_past": False,
    "start_date": datetime(2022, 2, 1),
    "email": ["X@X.com"],
    "email_on_failure": "C@X.com.br",
    "email_on_retry": "X@X.com.br",
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}

# Execução programada Às 22:00 em todos os dias da semana, de segunda a sábado.
@dag(
    dag_id="crawler_exportacao_comex",
    default_args=default_args,
    # captura de dados feita todos os dias da semana, 1x ao dia, às 17hs
    schedule_interval="0 20 * * 1-5", 
    tags=['crawler','X'],
    catchup=False,
    description="X"
) 


# Bloco que será executado como container 
def data_pipeline_comex():
    '''	Pipeline que utiliza os dados do ibge para carregar tabela no S3 e Athena.'''
    
    task_comex_dim_web_to_raw = KubernetesPodOperator(    
        task_id = 'executa_comex_dim_web_to_raw',
        namespace = 'airflow',
        image = 'X',
        arguments =  ['/app/crawler_exportacao_comex_dim_web_to_raw.py'],
        name = 'executa_comex_dim_web_to_raw',
        env_vars = {'AWS_ACCESS_KEY_ID': aws_access_key_id,
                    'AWS_SECRET_ACCESS_KEY': aws_secret_access_key},
        is_delete_operator_pod = True,
        in_cluster = True,
        get_logs = True,
        image_pull_policy='Always'
    )
    
    task_comex_dim_raw_to_staging = KubernetesPodOperator(    
        task_id = 'executa_comex_dim_raw_to_staging',
        namespace = 'airflow',
        image = 'X',
        arguments =  ['/app/crawler_exportacao_comex_dim_raw_to_staging.py'],
        name = 'executa_comex_dim_raw_to_staging',
        env_vars = {'AWS_ACCESS_KEY_ID': aws_access_key_id,
                    'AWS_SECRET_ACCESS_KEY': aws_secret_access_key},
        is_delete_operator_pod = True,
        in_cluster = True,
        get_logs = True,
        image_pull_policy='Always'
    )
    
    task_comex_dim_staging_to_consumer = KubernetesPodOperator(    
        task_id = 'executa_comex_dim_staging_to_consumer',
        namespace = 'airflow',
        image = 'X',
        arguments =  ['/app/crawler_exportacao_comex_dim_staging_to_consumer.py'],
        name = 'executa_comex_dim_staging_to_consumer',
        env_vars = {'AWS_ACCESS_KEY_ID': aws_access_key_id,
                    'AWS_SECRET_ACCESS_KEY': aws_secret_access_key},
        is_delete_operator_pod = True,
        in_cluster = True,
        get_logs = True,
        image_pull_policy='Always'
    )
    
    task_comex_web_to_raw = KubernetesPodOperator(    
        task_id = 'executa_comex_web_to_raw',
        namespace = 'airflow',
        image = 'X',
        arguments =  ['/app/crawler_exportacao_comex_web_to_raw.py'],
        name = 'executa_comex_web_to_raw',
        env_vars = {'AWS_ACCESS_KEY_ID': aws_access_key_id,
                    'AWS_SECRET_ACCESS_KEY': aws_secret_access_key},
        is_delete_operator_pod = True,
        in_cluster = True,
        get_logs = True,
        image_pull_policy='Always'
    )
    
    task_comex_raw_to_staging = KubernetesPodOperator(    
        task_id = 'executa_comex_raw_to_staging',
        namespace = 'airflow',
        image = 'X',
        arguments =  ['/app/crawler_exportacao_comex_raw_to_staging.py'],
        name = 'executa_comex_raw_to_staging',
        env_vars = {'AWS_ACCESS_KEY_ID': aws_access_key_id,
                    'AWS_SECRET_ACCESS_KEY': aws_secret_access_key},
        is_delete_operator_pod = True,
        in_cluster = True,
        get_logs = True,
        image_pull_policy='Always'
    )
    
    task_comex_staging_to_consumer = KubernetesPodOperator(    
        task_id = 'executa_comex_staging_to_consumer',
        namespace = 'airflow',
        image = 'X',
        arguments =  ['/app/crawler_exportacao_comex_staging_to_consumer.py'],
        name = 'executa_comex_staging_to_consumer',
        env_vars = {'AWS_ACCESS_KEY_ID': aws_access_key_id,
                    'AWS_SECRET_ACCESS_KEY': aws_secret_access_key},
        is_delete_operator_pod = True,
        in_cluster = True,
        get_logs = True,
        image_pull_policy='Always'
    )
    
    # control flow
    [[task_comex_dim_web_to_raw >> task_comex_dim_raw_to_staging >> task_comex_dim_staging_to_consumer], [task_comex_web_to_raw >> task_comex_raw_to_staging >> task_comex_staging_to_consumer]]

execucao=data_pipeline_comex()           
