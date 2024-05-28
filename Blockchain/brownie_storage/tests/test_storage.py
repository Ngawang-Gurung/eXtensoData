from brownie import Storage, accounts

def test_deploy():
    # Arrange
    account = accounts[0]
    # Act
    my_storage = Storage.deploy({"from":account})
    starting_value = my_storage.retrieve()
    expected = 0
    # Assert
    assert starting_value == expected

def test_updating_storage():
    # Arrange
    account = accounts[0]
    my_storage = Storage.deploy({"from":account})
    # Act
    expected = 15
    updated_value =  my_storage.store(expected, {"from":account})
    # Assert
    assert expected == my_storage.retrieve()
