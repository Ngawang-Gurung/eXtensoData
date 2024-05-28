from brownie import FundMe, config, network,MockV3Aggregator
from scripts.helper_functions import get_account, deploy_mocks, LOCAL_BLOCKCHAIN_ENVIRONMENTS

def deploy_fund_me():
    account = get_account()

    # pass the price feed address to our fundme contract
    # if we are on a persistent network like sepolia, use the associate address
    # otherwise, deploy mocks
    # if network.show_active() != 'development':
    if network.show_active() not in LOCAL_BLOCKCHAIN_ENVIRONMENTS:
        price_feed_address = config["networks"][network.show_active()]["eth_usd_price_feed"]
    else:
        deploy_mocks()
        price_feed_address = MockV3Aggregator[-1].address # Use most recently deployed MockV3Aggregator
    fund_me = FundMe.deploy(
        price_feed_address,
        {"from": account}, 
        publish_source = config["networks"][network.show_active()].get("verify")
        )
    print(f"Contract deployed to {fund_me.address}")
    return fund_me

def main():
    deploy_fund_me()