from etherscan import Etherscan
from sqlalchemy import create_engine
import pandas as pd
import utils


# Etherscan API key
class EVMConnector:

    def __init__(self, connector, wallet_address):
        self.pubkey, self.privkey = utils.load_keys(connector)
        self.wallet = wallet_address
        self.con = Etherscan(self.privkey)
    
    def balance(self):
        return float(self.con.get_eth_balance(self.wallet)) * 1E-18

    def last_block(self):
        self.timestamp = utils.current_timestamp()
        return self.con.get_block_number_by_timestamp(self.timestamp,
                                                      closest='before')
    # Build raw sql transactions database
    def _get_raw_transactions(self):
        
        # Check last block number
        self.lb = self.last_block()
        
        # create db engine
        self.engine = create_engine('sqlite:///raw/raw.sqlite', echo=False)
        
        # Pull all transactions
        try:
            print('getting normal transactions')
            self.n_tx = self.con.get_normal_txs_by_address(self.wallet, 0, self.lb, 'desc') # normal
            print('getting internal transactions')
            self.i_tx = self.con.get_internal_txs_by_address(self.wallet, 0, self.lb, 'desc') # internal
            print('getting erc20 transactions')
            self.erc20_tx = self.con.get_erc20_token_transfer_events_by_address(self.wallet, 0, self.lb, 'desc') # ERC20
            print('getting erc721 transactions')
            self.erc721_tx = self.con.get_erc721_token_transfer_events_by_address(self.wallet, 0, self.lb, 'desc') # ERC721
        except AssertionError as msg:
            print(msg)
        
        # Write data to raw database
        try:
            self.df_normal_tx = pd.DataFrame.from_dict(self.n_tx)
            self.df_normal_tx.to_sql(f'raw_{self.wallet}_normal_tx', con=self.engine, if_exists='replace')
            self.df_internal_tx = pd.DataFrame.from_dict(self.i_tx)
            self.df_internal_tx.to_sql(f'raw_{self.wallet}_internal_tx', con=self.engine, if_exists='replace')
            self.df_erc20_tx = pd.DataFrame.from_dict(self.erc20_tx)
            self.df_erc20_tx.to_sql(f'raw_{self.wallet}_erc20_tx', con=self.engine, if_exists='replace')
            self.df_erc721_tx = pd.DataFrame.from_dict(self.erc721_tx)
            self.df_erc721_tx.to_sql(f'raw_{self.wallet}_erc721_tx', con=self.engine, if_exists='replace')
        except AttributeError as msg:
            print(msg)
        # If there is more than 10k transactions, loop to add remaining transactions
        # There is probably a better way do do this
        try:
            if len(self.n_tx) > 10000:
                self.l_lb = self.n_tx[-self.size]['blockNumber']
                while True:
                    try:
                        self.n_tx = self.con.get_normal_txs_by_address(self.wallet,
                                                                    0,
                                                                    (int(self.l_lb)-1), 'desc')
                        self.df_normal_tx = pd.DataFrame.from_dict(self.n_tx)
                        self.df_normal_tx.to_sql(f'raw_{self.wallet}_normal_tx',
                                    con=self.engine, if_exists='append')
                        self.l_lb = self.n_tx[-1]['blockNumber']
                    except AssertionError as msg:
                        print(msg)
                        break
                    
            if len(self.i_tx) > 10000:
                self.l_lb = self.i_tx[-self.size]['blockNumber']
                while True:
                    try:
                        self.i_tx = self.con.get_internal_txs_by_address(self.wallet,
                                                                    0,
                                                                    (int(self.l_lb)-1), 'desc')
                        self.df_internal_tx = pd.DataFrame.from_dict(self.i_tx)
                        self.df_internal_tx.to_sql(f'raw_{self.wallet}_internal_tx',
                                    con=self.engine, if_exists='append')
                        self.l_lb = self.i_tx[-1]['blockNumber']
                    except AssertionError as msg:
                        print(msg)
                        break
            
            if len(self.erc20_tx) > 10000:
                self.l_lb = self.erc20_tx[-self.size]['blockNumber']
                while True:
                    try:
                        self.erc20_tx = self.con.get_erc20_token_transfer_events_by_address(self.wallet,
                                                                    0, (int(self.l_lb)-1), 'desc')
                        self.df_erc20_tx = pd.DataFrame.from_dict(self.erc20_tx)
                        self.df_erc20_tx.to_sql(f'raw_{self.wallet}_erc20_tx',
                                    con=self.engine, if_exists='append')
                        self.l_lb = self.erc20_tx[-1]['blockNumber']
                    except AssertionError as msg:
                        print(msg)
                        break
            
            if len(self.erc721_tx) > 10000:
                self.l_lb = self.erc721_tx[-self.size]['blockNumber']
                while True:
                    try:
                        self.erc721_tx = self.con.get_erc721_token_transfer_events_by_address(self.wallet,
                                                                    0, (int(self.l_lb)-1), 'desc')
                        self.df_erc721_tx = pd.DataFrame.from_dict(self.erc721_tx)
                        self.df_erc721_tx.to_sql(f'raw_{self.wallet}_erc721_tx',
                                    con=self.engine, if_exists='append')
                        self.l_lb = self.erc721_tx[-1]['blockNumber']
                    except AssertionError as msg:
                        print(msg)
                        break
        
        except AttributeError as msg:
            print(msg)
        
        return

        
               
class Ethereum(EVMConnector):
    pass
