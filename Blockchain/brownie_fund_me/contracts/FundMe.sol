// Get funds from users
// Withdraw funds
// Set a minimum funding value in USD

// Smart contract that lets anyone deposit ETH into the contract
// Only the owner of the contract can withdraw the ETH

// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

import "@chainlink/contracts/src/v0.8/shared/interfaces/AggregatorV3Interface.sol";

// How can we make the contract gas efficient?
// constant, immutable [variables that can't be changed]
// custom errors

// 756,759 gas - non-constant
// 736,398 gas - constant

error NotOwner();

contract FundMe {
    uint256 public constant MIN_USD = 1 * 1e18;
    // 303 gas - constant
    // 2402 gas - non-constant
    // 303*12000000000 = $0.01
    // 2402*12000000000 = $0.11

    address[] public funders;
    mapping(address => uint256) public addressToAmountFunded;

    address public i_owner;
    // 417 gas - immutable
    // 2552 gas - non-immutable

    AggregatorV3Interface public priceFeed;
    constructor(address _priceFeed) {
        priceFeed = AggregatorV3Interface(_priceFeed);
        i_owner = msg.sender;
    }

    function fund() public payable {
        // Want to be able to set a minimum fund amount in USD
        // 1. How do we send ETH to this contract
        require(getConversionRate(msg.value) >= MIN_USD, "Didn't send enough");
        // require(PriceConverter.getConversionRate(msg.value) >= MIN_USD, "Didn't send enough");
        funders.push(msg.sender);
        addressToAmountFunded[msg.sender] = msg.value;
    }

    function getPrice() internal view returns (uint256) {
        // Address of contract ETH to USD: 0x694AA1769357215DE4FAC081bf1f309aDC325306
        // ABI of contract
        // AggregatorV3Interface priceFeed = AggregatorV3Interface(
        //     0x694AA1769357215DE4FAC081bf1f309aDC325306
        // );
        (, int256 price, , , ) = priceFeed.latestRoundData();
        return uint256(price * 1e10);
    }

    // function getVersion() internal view returns (uint256) {
    //     // Address of contract: 0x694AA1769357215DE4FAC081bf1f309aDC325306
    //     // ABI of contract
    //     // AggregatorV3Interface priceFeed = AggregatorV3Interface(
    //     //     0x694AA1769357215DE4FAC081bf1f309aDC325306
    //     // );
    //     return priceFeed.version();
    // }

    function getConversionRate(uint256 ethAmt) internal view returns (uint256) {
        uint256 ethPrice = getPrice();
        uint256 ethAmtInUSD = (ethPrice * ethAmt) / 1e18;
        return ethAmtInUSD;
    }

    function getEntranceFee() public view returns (uint256) {
        // minimumUSD
        uint256 minimumUSD = 50 * 10 ** 18;
        uint256 price = getPrice();
        uint256 precision = 1 * 10 ** 18;
        return ((minimumUSD * precision) / price) + 1;
    }

    function withdraw() public onlyOwner {
        // require(msg.sender == owner,  "You cannot withdraw");
        // Start, End, Step
        for (uint256 funderIdx = 0; funderIdx < funders.length; funderIdx++) {
            address funder = funders[funderIdx];
            addressToAmountFunded[funder] = 0;
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
        (bool callSuccess, ) = payable(msg.sender).call{
            value: address(this).balance
        }("");
        require(callSuccess, "Call Failed");

        // msg.sender = address
        // payable(msg.sender) = payable address
    }

    modifier onlyOwner() {
        // require(msg.sender == i_owner,  "You cannot withdraw");
        if (msg.sender != i_owner) {
            revert NotOwner();
        }
        _; // This is the same as saying _ means the following code is executed
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
