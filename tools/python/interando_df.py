
import pandas as pd


dic = {'atacado': {0: 'Traseiro 1x1 capão',
  1: 'Dianteiro 1x1 capão',
  2: 'Traseiro 1x1 inteiro',
  3: 'Dianteiro 1x1 inteiro',
  4: 'Traseiro Avulso',
  5: 'Dianteiro Avulso',
  6: 'Ponta de Agulha (charque)',
  7: 'Ponta de Agulha (consumo) capão',
  8: 'Boi capão',
  9: 'Boi inteiro',
  10: 'Vaca casada',
  11: 'Novilha casada'},
 'preco_boi': {0: '20,75',
  1: '14,70',
  2: '19,95',
  3: '14,40',
  4: '19,35',
  5: '13,60',
  6: '13,50',
  7: '14,50',
  8: '17,58',
  9: '17,06',
  10: '16,00',
  11: '16,50'},
 'preco_vaca': {0: '',
  1: '',
  2: '',
  3: '',
  4: '',
  5: '',
  6: '',
  7: '',
  8: '',
  9: '',
  10: '',
  11: ''},
 'preco_ha1ano': {0: '25,15',
  1: '16,10',
  2: '21,50',
  3: '16,00',
  4: '23,30',
  5: '15,40',
  6: '13,80',
  7: '15,50',
  8: '20,37',
  9: '18,55',
  10: '18,50',
  11: '19,85'},
 'data': {0: '03-02-2023',
  1: '03-02-2023',
  2: '03-02-2023',
  3: '03-02-2023',
  4: '03-02-2023',
  5: '03-02-2023',
  6: '03-02-2023',
  7: '03-02-2023',
  8: '03-02-2023',
  9: '03-02-2023',
  10: '03-02-2023',
  11: '03-02-2023'}}


df = pd.DataFrame(dic)


for linha in df.itertuples():  
    # Adicionando a coluna vaca no data frame
    if linha.atacado=='Vaca casada' or linha.atacado=='Novilha casada':
        df.preco_vaca[linha.Index] = linha.preco_boi
        df.preco_boi[linha.Index] = '-'
    else:
        df.preco_vaca[linha.Index] = '-'

