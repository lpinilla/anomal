import pytest
import pandas as pd
import os
from data_drivers.CSVDriver import CSVInputDriver, CSVOutputDriver

dirname = os.path.dirname(__file__) + '/'

# input driver

def test_no_path_to_input_driver():
    input_driver = CSVInputDriver()
    with pytest.raises(ValueError):
        input_driver.setup('')
        input_driver.connect()
        input_driver.disconnect()

def test_load_csv_data():
    input_driver = CSVInputDriver()
    #create dict and save it as csv
    obj = {'key1': 'value1', 'key2': [2]}
    expected_df = pd.DataFrame(obj)
    csv_path = dirname + 'test.csv'
    expected_df.to_csv(csv_path, index=False)
    #setup input driver and get the data
    input_driver.setup(csv_path)
    input_driver.connect()
    real_df = input_driver.get_data()
    input_driver.disconnect()
    if os.path.exists(csv_path):
        os.remove(csv_path)
    assert expected_df.equals(real_df)

def test_get_fields():
    input_driver = CSVInputDriver()
    #create dict and save it as csv
    obj = {'key1': 'value1', 'key2': [2]}
    expected_df = pd.DataFrame(obj)
    csv_path = dirname + 'test.csv'
    expected_df.to_csv(csv_path, index=False)
    input_driver.setup(csv_path)
    input_driver.connect()
    #get data fields
    fields = input_driver.get_fields()
    input_driver.disconnect()
    if os.path.exists(csv_path):
        os.remove(csv_path)
    assert set(fields) == set(obj.keys())

# ouput driver

def test_no_path_to_output_driver():
    output_driver = CSVOutputDriver()
    with pytest.raises(ValueError):
        output_driver.setup('')
        output_driver.connect()

#create dataframe and save it using output driver
def test_dataframe_save():
    output_driver = CSVOutputDriver()
    obj = {'key1': 'value1', 'key2': [2]}
    expected_df = pd.DataFrame(obj)
    output_driver.setup(dirname)
    output_driver.connect()
    output_driver.save(expected_df, 'test.csv')
    output_driver.disconnect()
    csv_path = dirname + 'test.csv'
    real_df = pd.read_csv(csv_path, index_col=[0])
    if os.path.exists(csv_path):
        os.remove(csv_path)
    assert expected_df.equals(real_df)

def test_disconnect_exception_without_connect():
    output_driver = CSVOutputDriver()
    with pytest.raises(ConnectionError):
        output_driver.disconnect()

