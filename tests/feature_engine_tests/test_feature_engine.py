import base64
import pytest
import yaml
import sys
import types
from os import path, remove, urandom
from base64 import b64encode
import pandas as pd

from src.feature_engine.feature_engine import FeatureEngine
from src.data_drivers.CSVDriver import CSVInputDriver, CSVOutputDriver
from src.data_drivers.TCPDriver import TCPInputDriver
from rich.console import Console

dirname = path.dirname(__file__) + '/'

@pytest.fixture
def feature_engine():
    in_real_time = False
    console = Console()
    terminal_mode = False
    in_driver = CSVInputDriver()
    out_driver = CSVOutputDriver()
    features_path = ''
    save_results = False
    return FeatureEngine(in_driver, out_driver, in_real_time, features_path, save_results, console, terminal_mode)

@pytest.fixture
def df():
    #crate custom data objects
    obj1 = {
        'timestamp': 13,
        'bytes_in': 37,
        'bytes_out': 40
    }
    obj2 = {
        'timestamp': 40,
        'bytes_in': 22,
        'bytes_out': 19
    }
    objs = [obj1, obj2]
    #create dataframe from them
    return pd.DataFrame(objs)

def test_create_func_obj(feature_engine):
    string_code_true = 'def always_true(): return True'
    string_code_false = 'def always_false(): return False'
    always_true_fun_obj = feature_engine.create_func_obj(base64.b64encode(string_code_true.encode('ascii')))
    always_false_fun_obj = feature_engine.create_func_obj(base64.b64encode(string_code_false.encode('ascii')))
    assert always_true_fun_obj is not None
    assert always_false_fun_obj is not None
    assert always_true_fun_obj()
    assert not always_false_fun_obj()

def test_open_yaml(feature_engine):
    #check error on random file
    with pytest.raises(feature_engine.YamlIOError) as e:
        feature_engine.open_yaml('/temp/' + str(urandom(4)))
    #writing random yaml
    _path = dirname + 'yaml1_test.yaml'
    if path.exists(_path):
        remove(_path)
    expected = [{'feature1': ['bytes']}, {'feature2': ['bytes', 'number_count']}]
    with open(_path, 'w') as f:
        yaml.dump(expected, f)
    #reading and checking everything is ok
    result = feature_engine.open_yaml(_path)
    #cleaning file
    if path.exists(_path):
        remove(_path)
    assert result is not None
    assert expected == result

def test_plugin_imports(feature_engine):
    utils_str = 'def msg(): return \'utils imported\''
    utils_obj = feature_engine.create_func_obj(base64.b64encode(utils_str.encode('ascii')))
    assert utils_obj is not None
    aux_module = types.ModuleType('utils')
    #load the code into the module
    exec(utils_str, aux_module.__dict__)
    #load the module into sys
    sys.modules['utils'] = aux_module
    feature_engine.plugin_imports()
    #call the imported utils from feature_engine
    assert 'utils imported' == sys.modules['src.feature_engine'].feature_engine.utils.msg()

def test_check_data_needed_available(feature_engine):
    yaml_obj = {}
    yaml_obj['filters'] = [{'name': 'feature1', 'data_needed':  'timestamp'}]
    available_fields_mock = ['bytes']
    with pytest.raises(feature_engine.ValidationError) as e:
        feature_engine.check_data_needed_available(yaml_obj, available_fields_mock)
    #adding the timestamp as an available field
    available_fields_mock.append('timestamp')
    #run again, should get no errors
    try:
        feature_engine.check_data_needed_available(yaml_obj, available_fields_mock)
    except Exception as err:
        assert False, 'Exception was not expected %s' % err

def test_validate_filter(feature_engine):
    yaml_obj = {}
    #filter without name
    with pytest.raises(feature_engine.ValidationError) as e:
        feature_engine.validate_filter(yaml_obj, [])
    #add name
    yaml_obj['name'] = 'filter1'
    #has name but isn't implemented
    with pytest.raises(feature_engine.ValidationError) as e:
        feature_engine.validate_filter(yaml_obj, [])
    #doesn't have data_needed field
    with pytest.raises(feature_engine.ValidationError) as e:
        feature_engine.validate_filter(yaml_obj, ['filter1'])
    #added data_needed field
    yaml_obj['data_needed'] = 'timestamp'
    #has function_code, should not raise
    yaml_obj['function_code'] = ''
    try:
        feature_engine.validate_filter(yaml_obj, [])
    except Exception as e:
        assert False, e
    #erase function code
    del yaml_obj['function_code']
    #added data_needed field
    #defines params but they are empty
    yaml_obj['params'] = []
    with pytest.raises(feature_engine.ValidationError) as e:
        feature_engine.validate_filter(yaml_obj, ['filter1'])
    del yaml_obj['params']
    #should not raise any error now
    try:
        feature_engine.validate_filter(yaml_obj, ['filter1'])
    except Exception as e:
        assert False, e

def test_parse_filters(feature_engine):
    #create invalid filter
    _filter = {
        'name': 'filter1',
    }
    filter_code = 'def fil(): return \'working\''
    with pytest.raises(feature_engine.ValidationError) as e:
        feature_engine.parse_filters([_filter])
    #make filter valid
    _filter['data_needed'] = 'timestamp'
    _filter['function_code'] = b64encode(filter_code.encode('ascii'))
    try:
        parsed_filter = feature_engine.parse_filters([_filter])[0]
        assert parsed_filter['name'] == _filter['name']
        assert parsed_filter['fn']() == feature_engine.create_func_obj(base64.b64encode(filter_code.encode('ascii')))()
        assert parsed_filter['data_needed'] == _filter['data_needed']
        assert parsed_filter['params'] == {}
    except Exception as e:
        assert False, e

def test_get_module_implementations(feature_engine):
    #define the functions
    test_fn_1_str = 'def test_fn_1(): return \'fn_1 result\''
    test_fn_2_str = 'def test_fn_2(): return \'fn_2 result\''
    #define the functions
    #create a module
    aux_module = types.ModuleType('tests_fn')
    #add the functions to the module
    exec(test_fn_1_str, aux_module.__dict__)
    exec(test_fn_2_str, aux_module.__dict__)
    #get module implementations according to feature_engine
    result = feature_engine.get_module_implementations(aux_module)
    #expect it to be empty because module isn't in sys modules
    assert result == {}
    #adding module to sys
    sys.modules['tests_fn'] = aux_module
    result = feature_engine.get_module_implementations(aux_module)
    assert result is not None
    assert result != {}
    fn_names = result.keys()
    assert 'test_fn_1' in fn_names
    assert 'test_fn_2' in fn_names

def test_validate_feature(feature_engine):
    feature = {}
    #empty feature, should raise exception
    with pytest.raises(feature_engine.ValidationError) as e:
        feature_engine.validate_feature(feature, [])
    feature['name'] = 'feature1'
    #doesn't have type
    with pytest.raises(feature_engine.ValidationError) as e:
        feature_engine.validate_feature(feature, [])
    feature['type'] = 'random'
    #random type should should raise error
    with pytest.raises(feature_engine.ValidationError) as e:
        feature_engine.validate_feature(feature, [])
    feature['type'] = 'metric'
    #lacks data_needed field
    with pytest.raises(feature_engine.ValidationError) as e:
        feature_engine.validate_feature(feature, [])
    feature['data_needed'] = 'timestamp'
    #lacks multiplier
    with pytest.raises(feature_engine.ValidationError) as e:
        feature_engine.validate_feature(feature, [])
    feature['multiplier'] = 'random'
    #multiplier not int
    with pytest.raises(feature_engine.ValidationError) as e:
        feature_engine.validate_feature(feature, [])
    feature['multiplier'] = 3
    #unimplemented feature
    with pytest.raises(feature_engine.ValidationError) as e:
        feature_engine.validate_feature(feature, [])
    try:
        feature_engine.validate_feature(feature, ['feature1'])
    except Exception as e:
        assert False, e
    #not implemented but has function code
    feature['function_code'] = 'random'
    try:
        feature_engine.validate_feature(feature, [])
    except Exception as e:
        assert False, e

def test_parse_features(feature_engine):
    #create invalid feature
    _metric = {
        'name': 'metric1'
    }
    metric_code = 'def feat1(): return \'working metric\''
    with pytest.raises(feature_engine.ValidationError) as e:
        feature_engine.parse_features([_metric])
    #make feature valid
    _metric['data_needed'] = 'timestamp'
    _metric['function_code'] = b64encode(metric_code.encode('ascii'))
    _metric['type'] = 'metric'
    _metric['multiplier'] = 3
    #check if it can correctly create a metric
    try:
        parsed_features, parsed_metrics = feature_engine.parse_features([_metric])
        parsed_metrics = parsed_metrics[0]
        assert parsed_metrics['name'] == _metric['name']
        assert parsed_metrics['data_needed'] == _metric['data_needed']
        assert parsed_metrics['fn']() == feature_engine.create_func_obj(base64.b64encode(metric_code.encode('ascii')))()
        assert parsed_metrics['params'] == {}
    except Exception as e:
        assert False, e
    #check if it parses correctly a feature
    _feature = {
        'name': 'feature1'
    }
    feature_code = 'def feat1(): return \'working feature\''
    with pytest.raises(feature_engine.ValidationError) as e:
            feature_engine.parse_features([_feature])
    _feature['data_needed'] = 'timestamp'
    _feature['function_code'] = b64encode(feature_code.encode('ascii'))
    _feature['type'] = 'field'
    _feature['multiplier'] = 2
    try:
        parsed_features, parsed_metrics = feature_engine.parse_features([_feature])
        assert parsed_features[0] == _feature['name']
    except Exception as e:
        assert False, e


def test_validate_flags(feature_engine):
    flag = {}
    #flag with no name
    with pytest.raises(feature_engine.ValidationError) as e:
        feature_engine.validate_flag(flag, [])
    flag['name'] = 'flag1'
    #flag with no description
    with pytest.raises(feature_engine.ValidationError) as e:
        feature_engine.validate_flag(flag, [])
    flag['description'] = 'random flag'
    #flag with no type
    with pytest.raises(feature_engine.ValidationError) as e:
        feature_engine.validate_flag(flag, [])
    flag['type'] = 'direct'
    #flag with type != aggregation but no message
    with pytest.raises(feature_engine.ValidationError) as e:
        feature_engine.validate_flag(flag, [])
    flag['message'] = 'random message'
    #flag with no data_needed field
    with pytest.raises(feature_engine.ValidationError) as e:
        feature_engine.validate_flag(flag, [])
    flag['data_needed'] = 'timestamp'
    #flag with no severity
    with pytest.raises(feature_engine.ValidationError) as e:
        feature_engine.validate_flag(flag, [])
    flag['severity'] = 'random'
    #flag with unkown severity
    with pytest.raises(feature_engine.ValidationError) as e:
        feature_engine.validate_flag(flag, [])
    flag['severity'] = 'high'
    #flag not in implemented and no func obj
    with pytest.raises(feature_engine.ValidationError) as e:
        feature_engine.validate_flag(flag, [])
    #flag implemented, should work
    try:
        feature_engine.validate_flag(flag, ['flag1'])
    except Exception as e:
        assert False, e
    #name not found
    flag['function_code'] = 'random'
    try:
        feature_engine.validate_flag(flag, [])
    except Exception as e:
        assert False, e
    #same but has aggregation with no message
    flag['type'] = 'aggregation'
    del flag['message']
    try:
        feature_engine.validate_flag(flag, [])
    except Exception as e:
        assert False, e

def test_parse_flags(feature_engine):
    #create invalid flag
    _flag = {
        'name': 'flag1',
    }
    flag_code = 'def flag1(): return \'flag working\''
    with pytest.raises(feature_engine.ValidationError) as e:
        feature_engine.parse_flags([_flag])
    #make flag valid
    _flag['data_needed'] = 'timestamp'
    _flag['function_code'] = b64encode(flag_code.encode('ascii'))
    _flag['description'] = 'random description'
    _flag['type'] = 'direct'
    _flag['message'] = 'has run successfully'
    _flag['severity'] = 'high'
    try:
        parsed_flag = feature_engine.parse_flags([_flag])[0]
        assert parsed_flag['name'] == _flag['name']
        assert parsed_flag['description'] == _flag['description']
        assert parsed_flag['type'] == _flag['type']
        assert parsed_flag['data_needed'] == _flag['data_needed']
        assert parsed_flag['severity'] == _flag['severity']
        assert parsed_flag['fn']() == feature_engine.create_func_obj(base64.b64encode(flag_code.encode('ascii')))()
        assert parsed_flag['params'] == {}
        assert parsed_flag['message'] == _flag['message']
    except Exception as e:
        assert False, e

def test_apply_filters(feature_engine, df):
    obj2 = df.iloc[1]
    #create a filter that filters every field that has a timestamp < 40
    filter_code = 'def less_than_40(timestamp): return timestamp < 40'
    _filter = {
        'name': 'timestamp_less_than_40',
        'data_needed': 'timestamp',
        'function_code': b64encode(filter_code.encode('ascii'))
    }
    #parse the filter
    parsed_filter = feature_engine.parse_filters([_filter])
    #run the filter over the dataframe
    feature_engine.apply_filters(df, parsed_filter)
    #filter timestamp < 20
    assert len(df) == 1
    #get first row
    result = df.iloc[0]
    assert result['timestamp'] == obj2['timestamp']
    assert result['bytes_in'] == obj2['bytes_in']
    assert result['bytes_out'] == obj2['bytes_out']

def test_run_flags(feature_engine, df):
    #create flag's code
    flag_code = 'def timestamp_bigger_than_40_bytes_in_lower_than_30(timestamp, bytes_in): return (timestamp >= 40 and bytes_in < 30)'
    _flag = {
        'name': 'timestamp_less_than_40',
        'data_needed': 'timestamp,bytes_in',
        'function_code': b64encode(flag_code.encode('ascii')),
        'type': 'direct',
        'description': 'Fields with this params are sus',
        'message': 'These are the records discovered by the flag',
        'severity': 'low'
    }
    #parse flag
    parsed_flag = feature_engine.parse_flags([_flag])
    #run the flag over the df
    results = feature_engine.run_flags(df, parsed_flag)
    assert results is not None
    assert len(results) == 1
    result = results[0]
    assert result['name'] == _flag['name']
    assert result['description'] == _flag['description']
    assert result['type'] == _flag['type']
    assert result['data_needed'] == _flag['data_needed']
    assert result['severity'] == _flag['severity']
    assert result['params'] == {}
    assert result['message'] == _flag['message']
    assert 'fn' not in result
    sus_record = result['result'].iloc[0]
    assert df.iloc[1]['timestamp'] == sus_record['timestamp']
    assert df.iloc[1]['bytes_in']  == sus_record['bytes_in']
    assert df.iloc[1]['bytes_out'] == sus_record['bytes_out']

def test_apply_features(feature_engine, df):
    obj1 = df.iloc[0]
    obj2 = df.iloc[1]
    headers = list(df.columns)
    avg_function_code = 'def avg_fun(bytes_in, bytes_out): return int((bytes_in + bytes_out) / 2)'
    _fields = [{
        'name': 'timestamp',
        'multiplier': 2,
        'data_needed': 'timestamp',
        'type': 'field'
    },
    {
        'name': 'bytes_out',
        'multiplier': 3,
        'data_needed': 'bytes_out',
        'type': 'field'
    },
    {
        'name': 'avg_bytes',
        'multiplier': 2,
        'data_needed': 'bytes_in,bytes_out',
        'function_code': b64encode(avg_function_code.encode('ascii')) ,
        'type': 'metric'
    }]
    fields, metrics = feature_engine.parse_features(_fields)
    #define custom metric
    feature_engine.apply_features(df, fields, metrics, headers)
    assert 'timestamp' in df.columns
    assert 'bytes_out' in df.columns
    assert 'avg_bytes' in df.columns
    first_rec = df.iloc[0]
    second_rec = df.iloc[1]
    assert first_rec['timestamp'] == obj1['timestamp']
    assert second_rec['timestamp'] == obj2['timestamp']
    assert first_rec['bytes_out'] == obj1['bytes_out']
    assert second_rec['bytes_out'] == obj2['bytes_out']
    assert first_rec['avg_bytes'] == int((obj1['bytes_in'] + obj1['bytes_out']) / 2)
    assert second_rec['avg_bytes'] == int((obj2['bytes_in'] + obj2['bytes_out']) / 2)

def test_read_yaml(feature_engine):
    #define filter
    filter_code = 'def less_than_40(timestamp): return timestamp < 40'
    _filter = {
        'name': 'timestamp_less_than_40',
        'data_needed': 'timestamp',
        'function_code': b64encode(filter_code.encode('ascii'))
    }
    #define features
    avg_function_code = 'def avg_fun(bytes_in, bytes_out): return int((bytes_in + bytes_out) / 2)'
    _fields = [{
        'name': 'timestamp',
        'multiplier': 2,
        'data_needed': 'timestamp',
        'type': 'field'
    },
    {
        'name': 'bytes_out',
        'multiplier': 3,
        'data_needed': 'bytes_out',
        'type': 'field'
    },
    {
        'name': 'avg_bytes',
        'multiplier': 2,
        'data_needed': 'bytes_in,bytes_out',
        'function_code': b64encode(avg_function_code.encode('ascii')) ,
        'type': 'metric'
    }]
    #define flags
    flag_code = 'def timestamp_bigger_than_40_bytes_in_lower_than_30(timestamp, bytes_in): return (timestamp >= 40 and bytes_in < 30)'
    _flag = {
        'name': 'timestamp_less_than_40',
        'data_needed': 'timestamp,bytes_in',
        'function_code': b64encode(flag_code.encode('ascii')),
        'type': 'direct',
        'description': 'Fields with this params are sus',
        'message': 'These are the records discovered by the flag',
        'severity': 'low'
    }
    #TODO add plugin to test
    #create temp yaml file
    yaml_obj = {}
    yaml_obj['filters'] = [_filter]
    yaml_obj['features'] = _fields
    yaml_obj['flags'] = [_flag]
    temp_file_path = dirname + 'feature.yaml'
    #remove file if it exists
    if path.exists(temp_file_path):
        remove(temp_file_path)
    #write object to yaml
    with open(temp_file_path, 'w') as f:
        yaml.dump(yaml_obj, f, default_flow_style=False)
    #data needed should be in headers
    headers = ['timestamp','bytes_in','bytes_out']
    #load file and check that data preserved
    features, metrics, flags, filters = feature_engine.read_yaml(temp_file_path, headers)
    assert features is not None
    assert len(features) == 2
    assert features[0] == 'timestamp'
    assert features[1] == 'bytes_out'
    assert metrics is not None
    assert len(metrics) == 1
    assert metrics[0]['name'] == _fields[2]['name']
    assert metrics[0]['data_needed']==_fields[2]['data_needed']
    assert flags is not None
    assert len(flags) == 1
    assert flags[0]['name'] == _flag['name']
    assert flags[0]['severity'] == _flag['severity']
    assert flags[0]['data_needed'] == _flag['data_needed']
    assert flags[0]['type'] == _flag['type']
    assert filters is not None
    assert len(filters) == 1
    assert filters[0]['name'] == _filter['name']
    assert filters[0]['data_needed'] == _filter['data_needed']
    if path.exists(temp_file_path):
        remove(temp_file_path)

#def test_realtime_processing():
#   TODO
#   assert False

