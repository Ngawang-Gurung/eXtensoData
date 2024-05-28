// SPDX-License-Identifier: MIT
pragma solidity ^0.8.25;

contract Storage {
    int256 public num;
    
    struct People{
        int256 num;
        string name; 
    }
    // People public person1 = People({num: 10, name: "Hello"});

    // int256[] public numList; 
    People[] public people;

    mapping(int => string) public numToname;

    function store(int256 _num) public virtual {
        num = _num;
    }

    function retrieve() public view returns(int256){
        return num;
    }

    // function add() public pure returns(int256){
    //     return (1+1); 
    // }

    function addPerson(int256 _num, string memory _name) public {
        // People memory newPerson = People({num: _num, name: _name});
        // people.push(newPerson);
        people.push(People(_num, _name));
        numToname[_num] = _name; 
    }
}


