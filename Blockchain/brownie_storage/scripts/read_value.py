from brownie import Storage, accounts, config

def read_contract():
    # print(Storage)
    my_storage = Storage[-1]  # Most recent deployment
    # ABI
    # Address
    print(my_storage.retrieve())

def main():
    read_contract()