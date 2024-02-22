from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

aws_access_key_id = Variable.get("aws_access_key_id")
aws_secret_access_key = Variable.get("aws_secret_access_key")
IMAGE = 'x'
default_args = {
    'owner': 'PAULO',
    "depends_on_past": False,
    "start_date": datetime(2021, 8, 25),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id = 'crawler_pricing_ceagesp',
    default_args = default_args,
    # Atualizado somente na Segunda, Terça e Quinta, pois a cotacao só sai no dia seguinte a data coletada.
    schedule_interval = "0 13,17,21 * * 1,2,4",
    #schedule_interval = None,
    tags=['crawler','x'],
    catchup=False
) as dag:

    crawler_ceagesp = KubernetesPodOperator(
        task_id = "executa_crawler_ceagesp",
        namespace = 'airflow',
        image = IMAGE,
        arguments = ['/app/crawler_ceagesp.py'],
        name = 'execute-crawler-ceagesp',
        env_vars = {'AWS_ACCESS_KEY_ID': aws_access_key_id,
                    'AWS_SECRET_ACCESS_KEY': aws_secret_access_key},
        is_delete_operator_pod = True,
        in_cluster = True,
        get_logs = True,
        image_pull_policy='Always'
    )
    
    crawler_ceagesp
