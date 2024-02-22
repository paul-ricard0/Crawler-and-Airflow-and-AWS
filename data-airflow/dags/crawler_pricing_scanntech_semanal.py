from airflow.decorators import dag
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator


from datetime import datetime
from airflow.models import Variable

aws_access_key_id = Variable.get('aws_access_key_id')
aws_secret_access_key = Variable.get('aws_secret_access_key')

BUCKET_SCRIPTS = 'x-x'
IMAGE = 'x'
# Default settings applied to all tasks
default_args = {
    'owner': 'PAULO',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

@dag(
    default_args=default_args,
    dag_id='crawler_pricing_scanntech_semanal',
    schedule_interval='0 7 * * *', # Execução programada para todos os dias às 7 da manhã
    catchup=False,
    tags=['crawler', 'pricing'],
    description='Pipeline coleta os dados do scanntech via crawler subindo na raw-zone e para pasta no sharepoint da área de negócios dois arquivos csv, um para "as" e outro para "rota"',
)
# Bloco que será executado como container 
def data_pipeline_scanntech_semanal():
    '''	Pipeline que utiliza os dados da scanntech para carregar planilhas no S3 e Sharepoint.'''
    
    task_scanntech_semanal_as_web_to_raw = KubernetesPodOperator(    
        task_id = 'task_scanntech_semanal_as_web_to_raw',
        namespace = 'airflow',
        image = IMAGE, # revisar
        arguments =  ['/app/scanntech_semanal_as_crawler_to_raw.py'],
        name = 'task_scanntech_semanal_as_web_to_raw',
        env_vars = {'AWS_ACCESS_KEY_ID': aws_access_key_id,
                    'AWS_SECRET_ACCESS_KEY': aws_secret_access_key},
        is_delete_operator_pod = True,
        in_cluster = True,
        get_logs = True,
        image_pull_policy='Always'
    )

    task_scanntech_semanal_rota_web_to_raw = KubernetesPodOperator(    
        task_id = 'task_scanntech_semanal_rota_web_to_raw',
        namespace = 'airflow',
        image = IMAGE, # revisar
        arguments =  ['/app/scanntech_semanal_rota_crawler_to_raw.py'],
        name = 'task_scanntech_semanal_rota_web_to_raw',
        env_vars = {'AWS_ACCESS_KEY_ID': aws_access_key_id,
                    'AWS_SECRET_ACCESS_KEY': aws_secret_access_key},
        is_delete_operator_pod = True,
        in_cluster = True,
        get_logs = True,
        image_pull_policy='Always'
    )

    # control flow
    task_scanntech_semanal_as_web_to_raw >> task_scanntech_semanal_rota_web_to_raw

execucao=data_pipeline_scanntech_semanal()   
