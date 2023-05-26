from bs4 import BeautifulSoup
import urllib.request as ur
import pandas as pd
import requests
import os
from datetime import datetime

import sys
sys.path.append('D:\DataLake_MS')

from classes import get_secrets
from classes.connections import MinioClient

# Obtem as credenciais de acesso através da classe get_secrets
list_secrets = get_secrets.list_secrets()

# Define as urls a serem utilizadas
url_download = 'URL: http://www.dados.ms.gov.br/dataset/54e47a58-b9a2-48a1-9609-b425e1b95865/resource/'  #nfe-01_2018/download/201801_nfe.zip
url = 'http://www.dados.ms.gov.br/dataset/nota-fiscal-eletronica'

# Conecta no MinIO
minio = MinioClient(
    endpoint='192.168.15.8:9000',
    access_key=list_secrets['minio']['access_key'],
    secret_key=list_secrets['minio']['secret_key'],
    secure=False
)
minio.connect()

# Consulta no bucket quais arquivos já estão disponíveis
list_compras = minio.list_objects(bucket_name='raw', prefix='nfe/')
compras_arquivos = [item.replace('nfe/','').replace('.csv', '') for item in list_compras]
#print(compras_arquivos)

# Obtendo o código HTML para buscar os itens disponíveis
response = requests.get(url)
html = response.text
soup = BeautifulSoup(html, 'html.parser')
elementos = soup.find_all('li', class_='resource-item')
data_id_list = [li['data-id'] for li in elementos]
data_id_list = list(filter(lambda x: x.startswith("nfe-"), data_id_list))
data_id_list = [item.replace('nfe-','') for item in data_id_list]
#print(data_id_list)

# Criando um dataframe para identificar quais arquivos já foram baixados
df_status = pd.DataFrame({'data-id': data_id_list})
df_status['Raw'] = df_status['data-id'].apply(lambda x: 'X' if x in compras_arquivos else '')
df_status.loc[df_status['data-id'].isin(compras_arquivos), 'Raw'] = 'X'

# Criando a lista dos arquivos restantes
lista_download = df_status.loc[df_status['Raw'] == '']
lista_download = lista_download['data-id'].values.tolist()
#print(lista_download)

# Validar mes ano atual
data_atual = datetime.now()
ano_atual = data_atual.year
mes_atual = data_atual.month
mes_ano = "{:02d}_{}".format(mes_atual, ano_atual)
#print(mes_ano)

# Verifica os arquivos já disponíveis no lake e baixa os restantes
if lista_download is not None:
    for item in lista_download:
        if item != mes_ano:
            ano_mes_item = f'{item[3:]}{item[:2]}'
            file = ur.urlretrieve(f'{url_download}nfe-{item}/download/{ano_mes_item}.zip', f'.temp/{item}.zip') #nfe-01_2018/download/201801_nfe.zip
            name = item.replace('nfe-', '')
            minio.upload_file(
                bucket_name='raw',
                file_path=f'.temp/{item}.zip',
                object_name=f'nfe/{name}.zip'
            )
            os.remove(f'.temp/{item}.zip')
