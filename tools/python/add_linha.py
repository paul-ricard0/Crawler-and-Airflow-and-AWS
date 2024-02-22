import pandas as pd

# Criando um dataframe
df = pd.DataFrame({'Nome': ['João', 'Maria', 'Pedro'], 'Idade': [30, 25, 35]})

lista = ['PAULO', 32]
df.loc[len(df)] = lista

print(df)