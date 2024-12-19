import pandas as pd
from time import sleep
import logging
import sqlite3
logging.basicConfig(
                    filename=r'C:\Users\User1\Documents\School\TUC\data_sience\logfile.log',
                    format='[%(asctime)s][%(levelname)s] %(message)s',
                    level=logging.DEBUG,
                    datefmt='%Y-%m-%d %H:%M')

logging.info('Logging started.')
con = sqlite3.connect('bank_transaction.db')
logging.info('Connection to DB1 established')
con = sqlite3.connect('many_attmpts.db')
logging.info('Connection to DB2 established')

try:
    df = pd.read_csv(r'C:\Users\user1\Documents\School\TUC\programming\Datasets\archive\bank_transactions_data_2.csv', index_col = False)
except FileNotFoundError:
    logging.warning('File no found!')
except Exception as e: 
    logging.warning(e)
else:
    logging.info('Data_frame created') 
  
selected_row = df[df['LoginAttempts'] > 2]

df.to_sql('bank_transaction.db', con, if_exists='replace')
selected_row.to_sql('many_attempts', con, if_exists='replace')

logging.info('Done.')
