from brownie import accounts, config, Storage, network
# import os

def deploy_storage():
    account = get_account()
    # print(account)
    my_storage = Storage.deploy({"from":account})
    # print(my_storage)
    store_value = my_storage.retrieve()
    print(store_value)
    transcation = my_storage.store(15, {"from":account})
    transcation.wait(1)
    updated_stored_value = my_storage.retrieve()
    print(updated_stored_value)

def get_account():
    if network.show_active() == "development":
        return accounts[0]
    else:
        return accounts.add(config["wallets"]["from_key"])

def main():
    deploy_storage()



# Diffrent way to get account address
# account = accounts[0]
# account = accounts.load("my_account")
# account = accounts.add(os.getenv("PRIVATE_KEY"))
# account = accounts.add(config["wallets"]["from_key"])