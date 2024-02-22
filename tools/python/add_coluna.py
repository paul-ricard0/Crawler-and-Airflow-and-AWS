
import pandas as pd

'''  =========================  SUINO  ========================= '''
path = r'C:\X\dados'
arquivo = '\historico_suino.csv'

df = pd.read_csv(path+arquivo, encoding='ISO-8859-15', delimiter=',')

df.insert(0, "cepea_indicador", 'soja', True)