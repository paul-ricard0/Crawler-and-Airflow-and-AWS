FROM python:3.8

RUN mkdir __logger

# update and install sudo
RUN apt-get update
RUN apt-get install -y sudo

# Tabula precisa do Java
# instalando o java 
RUN apt install -y ghostscript python3-tk
RUN apt install -y ffmpeg libsm6 libxext6

RUN apt install -y openjdk-11-jdk
RUN export JAVA_HOME="/usr/lib/jvm/java-11-oracle"
RUN export PATH="$PATH:$JAVA_HOME/bin"


# Cria a pasta de downloads dentro da /app
RUN sudo mkdir -p /app/Downloads

# Scripts
COPY jox_crawler_to_raw.py /app
COPY jox_raw_to_staging.py /app
COPY jox_staging_to_consumer.py /app

# Libs
COPY requirements.txt /app

# Atualiza a versão do pip
RUN pip install --upgrade pip

# Camelot precisa disso
RUN apt-get update && apt-get install ffmpeg libsm6 libxext6  -y

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
