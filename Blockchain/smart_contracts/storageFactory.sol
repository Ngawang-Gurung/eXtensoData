// SPDX-License-Identifier: MIT
pragma solidity ^0.8.25;

import "./storage.sol";

contract StorageFactory{
    // Storage public s;
    Storage[] public sArr;

    function createStorageContract() public {
        Storage s = new Storage();
        sArr.push(s);
    }

    function sfStore(uint256 _sIdx, int256 _sNum) public {
        // Address of Contract
        // ABI - Application Binary Interface
        sArr[_sIdx].store(_sNum);
    }

    function sfGet(uint256 _sIdx) public view returns (int256) {
        return sArr[_sIdx].retrieve();
    }
}