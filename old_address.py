from etherscan import Etherscan
import datetime

# Etherscan API key
current_time = datetime.datetime.now()
current_timestamp = int(current_time.timestamp())

class Account:
    
    def __init__(self, wallet):
        self.wallet = wallet  
    
    # Returns ETH balance   
    def balance(self):
        b = eth.get_eth_balance(self.wallet)  
        return float(b) * 1E-18
    
    # Returns all transactions
    def normal_tx_all(self):
        bl = Blocks()
        last_block_num = bl.last_block()
        return eth.get_normal_txs_by_address(address=self.wallet, startblock=0, endblock=last_block_num, sort="desc")
    
    def internal_tx_all(self):
        bl = Blocks()
        last_block_num = bl.last_block()
        return eth.get_internal_txs_by_address(address=self.wallet, startblock=0, endblock=last_block_num, sort="desc")
    
    def erc20_tx_all(self):
        bl = Blocks()
        last_block_num = bl.last_block()
        return eth.get_erc20_token_transfer_events_by_address(address=self.wallet, startblock=0, endblock=last_block_num, sort="desc")

    def erc721_tx_all(self):
        bl = Blocks()
        last_block_num = bl.last_block()
        return eth.get_erc721_token_transfer_events_by_address(address=self.wallet, startblock=0, endblock=last_block_num, sort="desc")

    
class Blocks:
    
    def __init__(self):
        self.timestamp = current_timestamp
    
    def last_block(self):
        return eth.get_block_number_by_timestamp(self.timestamp, closest="before") 
        