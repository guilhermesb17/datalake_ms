import requests
import base64
import json
import os
from datetime import datetime, timedelta

import sys
sys.path.append('D:\DataLake_MS')

from classes import get_secrets
from classes.connections import MinioClient

# Obtem as credenciais de acesso através da classe get_secrets
list_secrets = get_secrets.list_secrets()

# Conecta no MinIO
minio_client = MinioClient(
    endpoint='192.168.15.8:9000',
    access_key=list_secrets['minio']['access_key'],
    secret_key=list_secrets['minio']['secret_key'],
    secure=False
)

minio_client.connect()

print(minio_client.list_buckets())