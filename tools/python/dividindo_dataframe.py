import pandas as pd


df  = pd.read_csv(r'C:\Users\X\historico_ibge_2021.csv', delimiter=',', encoding='ISO-8859-15')


df.drop_duplicates(inplace=True)

groups = df.groupby(df.ano_mes)

ano = '2021'

groups = df.groupby(df.ano_mes)

for mes in range(1,13):
    
    if mes < 10:
        mes = '0'+str(mes)
    else:
        mes = str(mes)
    print(ano+"-"+mes)
    df_temp = groups.get_group(ano+"-"+mes)
    
    df_temp.to_csv(r'C:\X\2021\ibge_'+ano+'_'+mes+'.csv', encoding='ISO-8859-15', index=False)


    
    







#df.to_csv(r'C:\Users\paulolima\OneDrive - Pif Paf Alimentos\Projetos\Scripts\Crawler\ipca_ibge\historico\historico_ibge_2020.csv', encoding='ISO-8859-15', index=False)


#del df['Unnamed: 0']