// Get funds from users
// Withdraw funds
// Set a minimum funding value in USD

// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

import "./PriceConverter.sol";

// How can we make the contract gas efficient?
    // constant, immutable [variables that can't be changed]
    // custom errors

// 756,759 gas - non-constant
// 736,398 gas - constant

error NotOwner();

contract FundMe {
    using PriceConverter for uint256;

    uint256 public constant MIN_USD = 1*1e18;
    // 303 gas - constant
    // 2402 gas - non-constant
    // 303*12000000000 = $0.01
    // 2402*12000000000 = $0.11

    address[] public funders;
    mapping(address => uint256) public addressToAmoundFuned;

    address public i_owner;
    // 417 gas - immutable
    // 2552 gas - non-immutable

    constructor(){
        i_owner = msg.sender;
    }

    function fund() public payable {
        // Want to be able to set a minimum fund amount in USD
        // 1. How do we send ETH to this contract
        require(msg.value.getConversionRate() >= MIN_USD, "Didn't send enough"); 
        // require(PriceConverter.getConversionRate(msg.value) >= MIN_USD, "Didn't send enough");
        funders.push(msg.sender);
        addressToAmoundFuned[msg.sender] = msg.value;
    }

    function withdraw() public onlyOwner{
        // require(msg.sender == owner,  "You cannot withdraw");
        // Start, End, Step
        for (uint256 funderIdx = 0; funderIdx < funders.length; funderIdx++){
            address funder = funders[funderIdx];
            addressToAmoundFuned[funder] = 0;
        }
        // Reset the array

        funders = new address[](0);

        // Actually withdraw the funds

        // transfer
        // payable(msg.sender).transfer(address(this).balance);
        // send
        // bool sendSuccess = payable(msg.sender).send(address(this).balance);
        // require(sendSuccess, "Sent Failed");
        // call
        (bool callSuccess,) = payable(msg.sender).call{value: address(this).balance}("");
        require(callSuccess, "Call Failed"); 

        // msg.sender = address
        // payable(msg.sender) = payable address
    }

    modifier onlyOwner(){
        // require(msg.sender == i_owner,  "You cannot withdraw");
        if(msg.sender != i_owner) {revert NotOwner();}
        _;  // This is the same as saying _ means the following code is executed
    }   

    // What happens if someone sends this contract ETH without calling the fund function?
    // receive()

    receive() external payable {
        fund();
     }

     fallback() external payable { 
        fund();
     }


    // fallback()
}

// Smart contracts can hold funds just like how wallets can 
// Reverting means undo any action before, and send remaining gas back