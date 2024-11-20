import os
from dotenv                         import load_dotenv
from kaggle.api.kaggle_api_extended import KaggleApi

load_dotenv()

os.getenv('KAGGLE_USERNAME')
os.getenv('KAGGLE_KEY')

api = KaggleApi()
api.authenticate()

dataset = 'olistbr/brazilian-ecommerce'

dataset_url = api.dataset_download_files(dataset, path='../data/', force=True, quiet=False, unzip=True)