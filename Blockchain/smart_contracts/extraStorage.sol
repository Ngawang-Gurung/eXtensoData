// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

import "./storage.sol";

contract ExtraStorage is Storage {
    // +10
    // override
    // virtual override

    function store(int256 _num) public override {
        num = _num + 10;   
    }
}




// Inorder for a function to be overrideable need to add virtual keyword to the orginal function