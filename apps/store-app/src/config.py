import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    KAFKA_BROKER = os.getenv('KAFKA_BROKER', '192.168.49.2:30092')
    CENSUS_DATA_TOPIC = os.getenv('CENSUS_DATA_TOPIC', 'census-data')
    GROUP_ID = os.getenv('GROUP_ID', 'store_app')
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', '1000'))
    BATCH_TIMEOUT_SECONDS = int(os.getenv('BATCH_TIMEOUT_SECONDS', '5'))
    S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL', 'http://192.168.49.2:31799/')
    S3_ACCESS_KEY_ID = os.getenv('S3_ACCESS_KEY_ID', 'CostaDiego')
    S3_SECRET_ACCESS_KEY = os.getenv('S3_SECRET_ACCESS_KEY', 'censusStorage')
    S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'validation-bucket')
