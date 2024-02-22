import pandas as pd 

'''  =========================  BOI_GORDO  ========================= '''
path = r'C:\X\historico\dados'
arquivo = '\historico_boi_gordo.csv'

df = pd.read_csv(path+arquivo, encoding='ISO-8859-15', delimiter=',')
print(df.head(2))

print('\n\nALTERANDO DATA DE LOCAL')
df2 = df[['local', 'a_vista_funrural', '30_dias_funrural',   'base', 'a_vista', '30_dias',   'data', 'ultima_atualizacao']]
print(df2.head(2))

print('\n\nUSANDO LISTA')
col = df.columns.tolist()
col.sort() # ordem alfabetica
df3 = df[col]
print(df3.head(2))


