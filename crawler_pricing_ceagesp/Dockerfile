# docker image python-slim
FROM python:3.8-slim-buster


# update and install sudo
RUN apt-get update
RUN apt-get install -y sudo


# create directory for crawler and Downloads folder
RUN sudo mkdir -p /app

# scripts
COPY crawler_ceagesp.py /app
COPY carga_ceagesp.py /app

# libs
COPY requirements.txt /app

# install dependencies
RUN pip install --no-cache -r /app/requirements.txt

# encoding for inconsistencies
ENV PYTHONIOENCODING=UTF-8 \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    TZ="America/Sao_Paulo"

# define app como diretorio de trabalho
WORKDIR /app

# Cria ponto de entrada para execução externa
ENTRYPOINT ["python"]
