from solcx import compile_standard, install_solc
import json

from web3 import Web3

from dotenv import load_dotenv
import os
load_dotenv()

with open("./contracts/storage.sol", "r") as file:
    storage_file = file.read()

# Compiler our Solidity
install_solc("0.8.19")
compiled_sol = compile_standard(
    {
        "language": "Solidity",
        "sources": {"storage.sol": {"content":storage_file}},
        "settings": {
            "outputSelection": {
                "*":{
                    "*": ["abi", "metadata", "evm.bytecode", "evm.bytecode.sourceMap"]
                }
            }
        }
    },
    solc_version = "0.8.19"
)

# print(compiled_sol)

with open("compiled_code.json", "w") as file:
    json.dump(compiled_sol, file)

# Deploying solidity

# Connecting to ganache
w3 = Web3(
    Web3.HTTPProvider(f"https://sepolia.infura.io/v3/{os.getenv("INFURA_API")}")
    )
chain_id = 11155111  
my_address = "0x0b9b6CC9e5899C65eA2fED0A027DCC94bC427F7E"
private_key = os.getenv("PRIVATE_KEY")

# Create contract in python
bytecode = compiled_sol["contracts"]["storage.sol"]["Storage"]["evm"]["bytecode"]["object"]
abi = compiled_sol["contracts"]["storage.sol"]["Storage"]["abi"]
Storage = w3.eth.contract(abi=abi, bytecode=bytecode)
# print(Storage)

# Get the latest transaction
nonce = w3.eth.get_transaction_count(my_address)
# print(nonce)

# 1. Build a transaction
# 2. Sign a transaction
# 3. Send a transaction

# Submit the transaction that deploys the contract
transaction = Storage.constructor().build_transaction(
    {
        "chainId": chain_id, 
        "gasPrice": w3.eth.gas_price,
        "from": my_address,
        "nonce":nonce,
    }
)
# print(transaction)

# Sign the transaction
signed_txn = w3.eth.account.sign_transaction(transaction, private_key = private_key)

# Send it!
print("Deploying contract...")
tx_hash = w3.eth.send_raw_transaction(signed_txn.raw_transaction)
tx_receipt = w3.eth.wait_for_transaction_receipt(tx_hash)
print("Deployed!")
# Working with the contract, you always need, contract Address and contract ABI

my_storage = w3.eth.contract(address = tx_receipt.contractAddress, abi = abi)

# Call -> Simulate making the call and getting a return value / Calls don't change block state
# Transact -> Actually make a state change

# Initial value of num
print(f"Initial Stored Value {my_storage.functions.retrieve().call()}")
# print(my_storage.functions.store(15).call())

print("Updating Contract... ")
store_transcation = my_storage.functions.store(15).build_transaction(
    {
        "chainId": chain_id,
        "from": my_address,
        "gasPrice": w3.eth.gas_price,
        "nonce": nonce + 1 # nonce can only be used once for each transaction
    }
)

signed_store_txn = w3.eth.account.sign_transaction(store_transcation, private_key = private_key)
store_tx_hash = w3.eth.send_raw_transaction(signed_store_txn.raw_transaction)
store_tx_receipt = w3.eth.wait_for_transaction_receipt(store_tx_hash)
print("Updated!")

print(my_storage.functions.retrieve().call())


