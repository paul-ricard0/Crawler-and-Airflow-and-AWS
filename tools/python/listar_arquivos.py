from os import walk



import os
for nome_arquivo in os.listdir(r'C:\Users\paulolima\X\historico\2020'):
    nome, extensao = os.path.splitext(nome_arquivo)
    print(nome, "  ", extensao)
    
