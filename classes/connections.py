from minio import Minio

class MinioClient:
    def __init__(self, endpoint, access_key, secret_key, secure=True):
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = secure

    def connect(self):
        self.client = Minio(
            endpoint=self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure
        )

    def create_bucket(self, bucket_name):
        self.client.make_bucket(bucket_name)

    def upload_file(self, bucket_name, file_path, object_name=None):
        if object_name is None:
            object_name = file_path.split("/")[-1]

        self.client.fput_object(bucket_name, object_name, file_path)
        
    def list_buckets(self):
        return self.client.list_buckets()

    def list_objects(self, bucket_name):
        return self.client.list_objects(Bucket=bucket_name)