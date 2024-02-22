import pandas as pd
import numpy as np

df  = pd.read_csv(r'C:\ZXXXXXXXX\historico_frango_vivo.csv', delimiter=',', encoding='ISO-8859-15')

df['nova_coluna'] = np.where(df['coluna'] == 'valor', 'se_for_verdade', 'se_for_falso')