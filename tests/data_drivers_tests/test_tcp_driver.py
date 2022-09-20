import pytest
import pandas as pd
import os
from data_drivers.TCPDriver import TCPInputDriver
import socket
import json
from datetime import date, datetime, timedelta
import time

dirname = os.path.dirname(__file__) + '/'

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))

# input driver

def test_empty_hostname_setup():
    input_driver = TCPInputDriver()
    with pytest.raises(ValueError):
        input_driver.setup('', 4040,0)
        input_driver.connect()
        input_driver.disconnect()

def test_incorrect_port_setup():
    input_driver = TCPInputDriver()
    with pytest.raises(ValueError):
        input_driver.setup('localhost', -10,0)
        input_driver.connect()
        input_driver.disconnect()

def test_incorrect_collection_time_setup():
    input_driver = TCPInputDriver()
    with pytest.raises(ValueError):
        input_driver.setup('localhost', 4040,-10)
        input_driver.connect()
        input_driver.disconnect()

def test_wrong_server_hostname_setup():
    input_driver = TCPInputDriver()
    input_driver.setup('localhos', 4041,0)
    with pytest.raises(socket.gaierror):
        input_driver.connect()
        input_driver.disconnect()

def test_server_can_start_ok():
    input_driver = TCPInputDriver()
    input_driver.setup('localhost', 4042,0)
    input_driver.connect()
    assert input_driver.disconnect()

def test_server_can_start_ok():
    input_driver = TCPInputDriver()
    input_driver.setup('localhost', 4042,0)
    input_driver.connect()
    assert input_driver.disconnect()

def test_server_online():
    #params
    HOST, PORT = 'localhost', 4043
    #server
    input_driver = TCPInputDriver()
    input_driver.setup(HOST, PORT,0)
    input_driver.connect()
    #client
    sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sock.connect((HOST, PORT))
    sock.close()
    assert input_driver.disconnect()


def test_client_read_fields():
    #params
    HOST, PORT = 'localhost', 4044
    #server
    input_driver = TCPInputDriver()
    input_driver.setup(HOST, PORT,0)
    input_driver.connect()
    #client
    sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sock.connect((HOST, PORT))
    raw_data = {
        "timestamp": str(datetime.now()),
        "foo": "first",
        "bar": 2
    }
    data = json.dumps(raw_data)
    sock.sendall(bytes(data,encoding="utf-8"))
    sock.close()
    assert set(raw_data.keys()) == set(input_driver.get_fields())
    assert input_driver.disconnect()

def test_multiple_client_read_fields():
    #params
    HOST, PORT = 'localhost', 4045
    #server
    input_driver = TCPInputDriver()
    input_driver.setup(HOST, PORT,0)
    input_driver.connect()
    #client 1
    sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sock.connect((HOST, PORT))
    raw_data = {
        "timestamp": str(datetime.now()),
        "foo": "first",
        "bar": 2
    }
    data = json.dumps(raw_data)
    sock.sendall(bytes(data,encoding="utf-8"))
    sock.close()
    #client 2
    sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sock.connect((HOST, PORT))
    raw_data_2 = {
        "timestamp": str(datetime.now()),
        "foo": "first",
        "bar": 2
    }
    data = json.dumps(raw_data_2)
    sock.sendall(bytes(data,encoding="utf-8"))
    sock.close()
    assert set(raw_data.keys()) == set(input_driver.get_fields())
    assert input_driver.disconnect()

def test_get_empty_data():
    #params
    HOST, PORT = 'localhost', 4046
    #server
    input_driver = TCPInputDriver()
    input_driver.setup(HOST, PORT, 0)
    input_driver.connect()
    assert input_driver.get_register() is None
    assert input_driver.disconnect()

def test_one_client_get_data():
    #params
    HOST, PORT = 'localhost', 4047
    #server
    input_driver = TCPInputDriver()
    input_driver.setup(HOST, PORT, 0)
    input_driver.connect()
    #client 1
    sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sock.connect((HOST, PORT))
    raw_data = {
        "timestamp": str(datetime.now()),
        "foo": "first",
        "bar": 2
    }
    data = json.dumps(raw_data)
    sock.sendall(bytes(data,encoding="utf-8"))
    sock.close()
    expected_df = pd.DataFrame(raw_data, index=[0])
    time.sleep(0.1)
    result_df = input_driver.get_register()
    assert result_df is not None
    assert expected_df.reset_index(drop=True).equals(result_df.reset_index(drop=True))
    #check cache is empty
    assert input_driver.get_register() is None
    assert input_driver.disconnect()

def test_multiple_client_get_data():
    #params
    HOST, PORT = 'localhost', 4048
    #server
    input_driver = TCPInputDriver()
    input_driver.setup(HOST, PORT, 0)
    input_driver.connect()
    #client 1
    sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sock.connect((HOST, PORT))
    raw_data = {
        "timestamp": str(datetime.now()),
        "foo": "first",
        "bar": 2
    }
    data = json.dumps(raw_data)
    sock.sendall(bytes(data,encoding="utf-8"))
    sock.close()
    expected_df = pd.DataFrame(raw_data, index=[0])
    #client 2
    sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sock.connect((HOST, PORT))
    raw_data_2 = {
        "timestamp": str(datetime.now() + timedelta(1000)),
        "foo": "first",
        "bar": 2
    }
    data_2 = json.dumps(raw_data_2)
    sock.sendall(bytes(data_2,encoding="utf-8"))
    sock.close()
    expected_df_2 = pd.DataFrame(raw_data_2, index=[0])
    time.sleep(0.1)
    #check response 1
    result_df = input_driver.get_register()
    assert result_df is not None
    assert expected_df.reset_index(drop=True).equals(result_df.reset_index(drop=True))
    #check response 2
    result_df_2 = input_driver.get_register()
    assert result_df_2 is not None
    assert expected_df_2.reset_index(drop=True).equals(result_df_2.reset_index(drop=True))
    #check cache is empty
    assert input_driver.get_register() is None
    assert input_driver.disconnect()
