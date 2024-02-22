
import pyathena
import pandas as pd
from os import environ

# Credenciais aws
aws_access_key_id = environ['AWS_ACCESS_KEY_ID']
aws_secret_access_key = environ['AWS_SECRET_ACCESS_KEY']


athena_conn = pyathena.connect(aws_access_key_id=aws_access_key_id, 
                 aws_secret_access_key=aws_secret_access_key, 
                 s3_staging_dir='s3://aws-athena-query-results-X-us-east-1/', 
                 region_name='us-east-1') 

print('CONXÃO FEITA COM SUCESSO!!!')

query = """
SELECT * FROM pricing.jox_frango_vivo
ORDER BY ultima_atualizacao  DESC
"""


df = pd.read_sql(query, athena_conn)


print(df)
