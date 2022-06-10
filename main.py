import connector

if __name__ == "__main__":

    wallets = ['',
               '']

    for w in wallets:
        eth = connector.Ethereum('etherscan', w)
        print(f'Pulling {w} transactions')
        eth._get_raw_transactions()


    '''
    print(f'public key:{eth.pubkey}, private key: {eth.privkey}')
    print(f'wallet address:{eth.wallet}')
    if eth.con:
        print('Connection established.')
    else:
        print('Connection failed.')
    print(f'balance: {eth.balance()} ETH')
    print(f'last block: {eth.last_block()}')
    '''

    