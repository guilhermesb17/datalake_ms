from yaml import load, SafeLoader

def list_secrets():
    ## Definições das chaves
    with open('secrets.yaml', 'r') as file:
            secrets = load(file, Loader=SafeLoader)

    minio_access_key = secrets['minio']['access_key']
    minio_secret_key = secrets['minio']['secret_key']

    secrets_f = {
        'minio':{
          'access_key': minio_access_key,
          'secret_key': minio_secret_key,
        },
    }

    return secrets_f