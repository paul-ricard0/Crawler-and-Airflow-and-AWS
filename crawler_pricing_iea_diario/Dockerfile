FROM python:3.8
RUN mkdir __logger

# Install Chrome
RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
RUN dpkg -i google-chrome-stable_current_amd64.deb; apt-get -fy install

# Adicionando chaves ao apt dos repositorios google
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -

# Adicionando Google Chrome aos repositorios
RUN sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list'

# Atualizando o apt para que instale o google chrome
RUN apt-get -y update
# Instala o navegador
RUN apt-get install -y google-chrome-stable

# Instala a lib unzip para abrir arquivos zipados
# Instala a opção sudo para mudar a pasta de downloads
# Instala a opção xvfb para o crawler
RUN apt-get install -yqq unzip sudo xvfb
# Instala o comando que muda a pasta de downloads
RUN sudo apt-get install -y xdg-user-dirs

# Baixa o Chrome Driver versão mais recente
RUN wget -O /tmp/chromedriver.zip https://chromedriver.storage.googleapis.com/111.0.5563.64/chromedriver_linux64.zip

# Descompacta o Chrome Driver na pasta /usr/local/bin directory
RUN unzip /tmp/chromedriver.zip chromedriver -d /usr/bin/
# Seta permissão de execução para o chromedriver
RUN sudo chown root:root /usr/bin/chromedriver
RUN sudo chmod +x /usr/bin/chromedriver

# Download required jar files for selenium
RUN wget https://selenium-release.storage.googleapis.com/3.13/selenium-server-standalone-3.13.0.jar
RUN wget http://www.java2s.com/Code/JarDownload/testng/testng-6.8.7.jar.zip
RUN unzip testng-6.8.7.jar.zip

# Marca a porta 99 como uma variavel de ambiente (necessária para evitar crashes em alguns casos)
#ENV DISPLAY=:99
# Cria a pasta de downloads dentro da /app
RUN sudo mkdir -p /app/Downloads

# Scripts
COPY crawler_pricing_iea_diario_crawler_to_raw.py /app
COPY crawler_pricing_ieadiario_raw_to_staging.py /app
COPY crawler_pricing_ieadiario_staging_to_consumer.py /app

# Libs
COPY requirements.txt /app
# Atualiza a versão do pip
RUN pip install --upgrade pip



# Instala as dependencias no container
RUN pip install -r /app/requirements.txt

# Encoding for inconsistencies
ENV PYTHONIOENCODING=UTF-8 \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    CHROMEDRIVER_DIR=/usr/bin

# Altera pasta de download padrão
RUN sudo xdg-user-dirs-update --set DOWNLOAD /app/Downloads

# Define app como diretorio de trabalho
WORKDIR /app

# Cria ponto de entrada para execução externa
ENTRYPOINT ["python"]
