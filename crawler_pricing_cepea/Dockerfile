FROM python:3.8

RUN mkdir __logger

# update and install sudo
RUN apt-get update
RUN apt-get install -y sudo

# Cria a pasta de downloads dentro da /app
RUN sudo mkdir -p /app/Downloads

# Scripts
COPY crawler_cepea_to_raw_to_staging.py /app
COPY crawler_cepea_staging_to_consumer.py /app

# Libs
COPY requirements.txt /app

# Atualiza a versão do pip
RUN pip install --upgrade pip

# Instala as dependencias no container
RUN pip install -r /app/requirements.txt

# Encoding for inconsistencies
ENV PYTHONIOENCODING=UTF-8 \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 

# Define app como diretorio de trabalho
WORKDIR /app

# Cria ponto de entrada para execução externa
ENTRYPOINT ["python"]
