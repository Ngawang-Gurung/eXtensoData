// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

import "@chainlink/contracts/src/v0.8/shared/interfaces/AggregatorV3Interface.sol";

library PriceConverter{
    function getPrice() internal  view returns (uint256) {
        // Address of contract: 0x694AA1769357215DE4FAC081bf1f309aDC325306
        // ABI of contract
        AggregatorV3Interface priceFeed = AggregatorV3Interface(0x694AA1769357215DE4FAC081bf1f309aDC325306);
        (, int256 price, , , )  = priceFeed.latestRoundData();
        return uint256(price * 1e10);
    }

    function getVersion() internal  view returns (uint256){
        // Address of contract: 0x694AA1769357215DE4FAC081bf1f309aDC325306
        // ABI of contract
        AggregatorV3Interface priceFeed = AggregatorV3Interface(0x694AA1769357215DE4FAC081bf1f309aDC325306);
        return priceFeed.version();
    }

    function getConversionRate(uint256 ethAmt) internal  view returns (uint256) {
        uint256 ethPrice = getPrice();
        uint256 ethAmtInUSD = (ethPrice*ethAmt) / 1e18;
        return ethAmtInUSD;
    }
}