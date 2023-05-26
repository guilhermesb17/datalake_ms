from bs4 import BeautifulSoup
import urllib.request as ur
import pandas as pd
import requests
import os

import sys
sys.path.append('D:\DataLake_MS')

from classes import get_secrets
from classes.connections import MinioClient

# Obtem as credenciais de acesso através da classe get_secrets
list_secrets = get_secrets.list_secrets()

# Define as urls a serem utilizadas
url_download = 'http://www.dados.ms.gov.br/datastore/dump/'
url = 'http://www.dados.ms.gov.br/dataset/receitas-mensal'

# Conecta no MinIO
minio = MinioClient(
    endpoint='192.168.15.8:9000',
    access_key=list_secrets['minio']['access_key'],
    secret_key=list_secrets['minio']['secret_key'],
    secure=False
)
minio.connect()

# Consulta no bucket quais arquivos já estão disponíveis
list_arquivos = minio.list_objects(bucket_name='raw', prefix='receitas/')
arquivos = [item.replace('receitas/','').replace('.csv', '') for item in list_arquivos]

# Obtendo o código HTML para buscar os itens disponíveis
response = requests.get(url)
html = response.text
soup = BeautifulSoup(html, 'html.parser')
elementos = soup.find_all('li', class_='resource-item')
data_id_list = [li['data-id'] for li in elementos]
data_id_list = [item.replace('receitas-','') for item in data_id_list]

# Criando um dataframe para identificar quais arquivos já foram baixados
df_status = pd.DataFrame({'data-id': data_id_list})
df_status['Raw'] = df_status['data-id'].apply(lambda x: 'X' if x in arquivos else '')
df_status.loc[df_status['data-id'].isin(arquivos), 'Raw'] = 'X'

# Criando a lista dos arquivos restantes
lista_download = df_status.loc[df_status['Raw'] == '']
lista_download = lista_download['data-id'].values.tolist()

# Verifica os arquivos já disponíveis no lake e baixa os restantes
if lista_download is not None:
    for item in lista_download:
        file = ur.urlretrieve(f'{url_download}receitas-{item}', f'.temp/{item}.csv')
        name = item.replace('receitas-', '')
        minio.upload_file(
            bucket_name='raw',
            file_path=f'.temp/{item}.csv',
            object_name=f'receitas/{name}.csv'
        )
        os.remove(f'.temp/{item}.csv')
