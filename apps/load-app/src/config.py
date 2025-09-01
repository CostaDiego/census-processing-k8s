import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    KAFKA_BROKER = os.getenv('KAFKA_BROKER', '192.168.49.2:30092')
    REQUEST_TOPIC = os.getenv('REQUEST_TOPIC', 'census-requests')
    CENSUS_DATA_TOPIC = os.getenv('CENSUS_DATA_TOPIC', 'census-data')
    GROUP_ID = os.getenv('GROUP_ID', 'load_app')
    S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL', 'http://192.168.49.2:31799/')
    S3_ACCESS_KEY_ID = os.getenv('S3_ACCESS_KEY_ID', 'CostaDiego')
    S3_SECRET_ACCESS_KEY = os.getenv('S3_SECRET_ACCESS_KEY', 'censusStorage')
