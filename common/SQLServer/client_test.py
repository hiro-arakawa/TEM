import pytest
import pyodbc
from common.SQLServer.client import ConnectionFactory

@pytest.fixture
def connection_factory():
    return ConnectionFactory(server="test_server", database="test_db", username="test_user", password="test_password")

def test_connection_string_initialization(connection_factory):
    expected_connection_string = (
        "DRIVER={SQL Server};"
        "SERVER=test_server;"
        "DATABASE=test_db;"
        "UID=test_user;"
        "PWD=test_password;"
    )
    assert connection_factory.connection_string == expected_connection_string

def test_create_connection(mocker, connection_factory):
    mock_connect = mocker.patch("pyodbc.connect")
    connection_factory.create_connection()
    mock_connect.assert_called_once_with(connection_factory.connection_string)