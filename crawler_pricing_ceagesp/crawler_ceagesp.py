### CRAWLER CEAGESP
## Executar os crawler na Segunda, TerÃ§a e Quinta. 
## Para coletar os dados na data dos dias de Sexta, Segunda e Quarta.

import pandas as pd
import requests, os, time, datetime, warnings, boto3, json #, pymsteams
import awswrangler as wr


# Ignorando os alertas de warnings
warnings.filterwarnings("ignore")


# VariÃ¡veis de ambiente
aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']



# Criando a sessÃ£o do Boto3
session = boto3.Session(
    region_name = "us-east-1",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

'''
# Configurando o Bot alert
aws_secrets = boto3.client("secretsmanager", region_name="us-east-1")

teams_secrets = json.loads(
    aws_secrets.get_secret_value(SecretId="Teams-BotAlertas")["SecretString"]
)

def send_teams_alert(alert):
    myTeamsMessage = pymsteams.connectorcard(teams_secrets["url_teams_botalertas"])
    myTeamsMessage.text(alert)
    myTeamsMessage.send()
'''


print('### Checando a data do dia para coletar os dados # Funcao para coletar a data de ontem ou de Sexta')
def checadata(xdata):
    if xdata == 'ontem':
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
    elif xdata == 'sexta':
        yesterday = datetime.date.today() - datetime.timedelta(days=3)
    
    return yesterday.strftime('%d/%m/%Y')



print('### Verificando a data de hoje pra coletando os dados de Ontem ou Sexta')

datadodia = ''    

# obtem a data de Sexta
if datetime.datetime.today().weekday() == 0:
    datadodia = checadata('sexta')
# obtem a data de Segunda
elif datetime.datetime.today().weekday() == 1:
    datadodia = checadata('ontem')
# obtem a data de Quarta
elif datetime.datetime.today().weekday() == 3:
    datadodia = checadata('ontem')
else:
    print('ImpossÃ­vel capturar dados hoje!')
    exit()



print('### Obtendo HTML para extraÃ§Ã£o dos dados ###')

params = {'cot_grupo': 'PESCADOS', 'cot_data': datadodia}

html = requests.post("https://ceagesp.gov.br/cotacoes/#cotacao", data=params)

table = pd.read_html(html.content)

print('# Capturando a Data do html')
data = table[0].iloc[0][0][-10:]

print('# FunÃ§Ã£o que retorna a data.valor() igual a do excel, para gerar a coluna chave')
def datavalor(data):
    inicial = datetime.datetime.strptime('01/01/1900', '%d/%m/%Y')
    final = datetime.datetime.strptime(data, '%d/%m/%Y')
    return final.toordinal() - inicial.toordinal()+2


print('# Checando se foi coletado a data corretamento do site.')
try:
    datavalor(data)
except Exception as e:
    print(f'Erro ao coletar dados no dia {datadodia}, provavelmente nÃ£o existe cotaÃ§Ã£o nesta data! \nErro: {e}')
    #send_teams_alert(f'>>>>>>>>>> Erro ao coletar dados do Crawler Ceagesp, provavelmente o site estÃ¡ indisponÃ­vel, novas tentativas de coleta serÃ¡ feita durante o dia! <<<<<<<<<<')
    exit()
else:
    print(f'Dados coletados corretamente no dia: {data}!')



print('### Salvar HTML na Raw ###')

# Pra gerar o nome do arquivo raw de acordo com a data coletada no html.
convert_data = datetime.datetime.strptime(data, '%d/%m/%Y').date()
data_raw = convert_data.strftime('%Y-%m-%d')
nome_raw = data_raw+datetime.datetime.fromtimestamp(time.time()).strftime('-%H-%M')+'.html'

# Salvando o arquivo raw localmente
with open(nome_raw, 'wb') as f:
    f.write(html.content)

# Fazendo o upload para a camada Raw
wr.s3.upload(
    local_file = nome_raw, 
    path = f's3://XXXXXXX80336967/raw-zone/crawlers/pricing/ceagesp/fat/{nome_raw}', 
    boto3_session = session)

print(f'Upload do arquivo Raw {nome_raw} gravado com sucesso!')


print('### Criando o Dataframe dos dados coletados ###')

df1 = table[0][1:]

df1.columns = ['produto', 'classificacao','unidade', 'menor', 'media', 'maior', 'quilo']
df2 = df1[1:]

hora_atual = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

df2.insert(0, 'indicadores', 'CEAGESP', allow_duplicates=False)
df2.insert(1, 'chave', 'Pescados'+ str(datavalor(data)), allow_duplicates=False)
df2.insert(2, 'tipo_de_proteina', 'Pescados', allow_duplicates=False)
df2.insert(3, 'data', data, allow_duplicates=False)
df2['ultima_atualizacao'] = hora_atual



print('# Formantando as colunas para os tipos de dados corretos')
df2[['menor', 'media', 'maior', 'quilo']] = df2[['menor', 'media', 'maior', 'quilo']].astype(float)
df2[['indicadores', 'chave','tipo_de_proteina', 'produto', 'classificacao', 'unidade']] = df2[['indicadores', 'chave','tipo_de_proteina', 'produto', 'classificacao', 'unidade']].astype("string")
df2['data'] = pd.to_datetime(df2['data'], format='%d/%m/%Y', errors='coerce')
df2['ultima_atualizacao'] = pd.to_datetime(df2['ultima_atualizacao'], format='%Y-%m-%d %H:%M:%S', errors='coerce')



#  Escrevendo o Dataframe ceagesp em Csv no S3
wr.s3.to_csv(
    df = df2,
    path = 's3://XXXXXXXXX80336967/raw-zone/crawlers/pricing/ceagesp/fat/',
    dataset = True,
    mode = 'append', 
    boto3_session = session,
    index = False
)

# Crawler executado com sucesso!
print(f'Cotação do Ceagesp atualizada com sucesso as: {hora_atual}!')
