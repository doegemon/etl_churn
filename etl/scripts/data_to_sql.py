import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

data_path = '../data/'

files_list = [f for f in os.listdir(data_path) if f.endswith('.csv')]

db_url = os.getenv('DB_URL')
engine = create_engine(db_url)

for file in files_list: 
  print(file)  
  df = pd.read_csv(f'../data/{file}')

  file_format = os.path.splitext(file)[0]
  
  df.to_sql(name=f'{file_format}', con=engine, if_exists='replace', index=False)
  
  print(f'The {file_format} table was created in the database.')