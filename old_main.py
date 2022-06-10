from etherscan import Etherscan
from old_address import Account, Blocks
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
from connector import load_keys

wallet = ""

# Initialize the API
acc = Account(wallet)

# Pull wallet ETH balance
acc.balance()
print(acc.balance(), "ETH")

# Pull last valid Ethereum block
block = Blocks()
print(block.last_block())
 
 # Pull all normal transactions
all_tx = acc.normal_tx_all()
print('Pulling Normal transactions')

# Clean normal transactions data
print('Cleaning Data')
# Build and clean normal transactions data
df_normal_tx = pd.DataFrame(all_tx)
# Set hash as index
df_normal_tx.set_index('hash', inplace=True)
# Drop unneeded information
df_normal_tx.drop(columns=['blockNumber',
                           'blockHash', 
                           'transactionIndex', 
                           'gas',
                           'gasPrice',
                           'txreceipt_status',
                           'cumulativeGasUsed',
                           'confirmations'],
                  inplace=True)
# Add information if the transaction is incoming or outgoing
df_normal_tx['In/Out'] = np.where(df_normal_tx['to'] == wallet, 'IN', 'OUT')
# Convert values to numbers 
df_normal_tx['value'] = df_normal_tx['value'].apply(pd.to_numeric)
df_normal_tx['gasUsed'] = df_normal_tx['gasUsed'].apply(pd.to_numeric)
# Add decimal point
df_normal_tx['value'] = 1E-18 * df_normal_tx['value']
df_normal_tx['gasUsed'] = 1E-18 * df_normal_tx['gasUsed']

# Trim Input Data into MethodId
df_normal_tx['input'] = df_normal_tx['input'].str.slice(stop=10)
print(df_normal_tx['input'])
# Save to csv file
# df_normal_tx.to_csv(f"{wallet}_normal_tx.csv")

# Create SQL table for normal transaction
"""
disk_engine = create_engine('sqlite:///raw/store.sqlite')


with disk_engine.connect() as conn:
     conn.execute('''CREATE TABLE IF NOT EXISTS `normal_tx` (
         `hash` TEXT NOT NULL, 
         `timeStamp` TEXT, 
         `nonce` TEXT, 
         `from` TEXT, 
         `to` TEXT, 
         `value` FLOAT, 
         `isError`	TEXT, 
         `input` TEXT, 
         `contractAddress` TEXT, 
         `gasUsed` FLOAT, 
         `In/Out` TEXT, 
         CONSTRAINT `hash_id` PRIMARY KEY (`hash`)
         );''')


df_normal_tx.to_sql('normal_tx', disk_engine, if_exists='append')
"""
   





print(df_normal_tx.head())

'''
internal_tx = acc.internal_tx_all()
erc20_tx = acc.erc20_tx_all()
erc721_tx = acc.erc721_tx_all()



df_internal_tx = pd.DataFrame(internal_tx)
df_internal_tx.to_csv(f"{wallet}_internal_tx.csv")
print(df_internal_tx.head())

df_erc20_tx = pd.DataFrame(erc20_tx)
df_erc20_tx.to_csv(f"{wallet}_erc20_tx.csv")
print(df_erc20_tx.head())

df_erc721_tx = pd.DataFrame(erc20_tx)
df_erc721_tx.to_csv(f"{wallet}_erc721_tx.csv")
print(df_erc20_tx.head())
'''