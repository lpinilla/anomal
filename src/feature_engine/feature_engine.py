import pandas as pd
from base64 import b64encode, b64decode
import requests
import json
import yaml
import inspect
import time
import sys
import pickle

from .plugin_loader import PluginLoader

class FeatureEngine():

    in_driver = None
    out_driver = None
    save_results = None
    features_path = None
    in_real_time = None
    pluginLoader = None
    console = None
    terminal_mode = None

    #maybe separate into errors module
    class ValidationError(Exception):
        def __init__(self, msg):
            self.msg = msg

    class YamlIOError(Exception):
        def __init__(self, msg):
            self.msg = msg

    def __init__(self, in_driver, out_driver, in_real_time, features_path, save_results, console, terminal_mode):
        self.in_driver = in_driver
        self.in_real_time = in_real_time
        self.out_driver = out_driver
        self.features_path = features_path
        self.save_results = save_results
        self.plugin_loader = PluginLoader(console, terminal_mode)
        self.console = console
        self.terminal_mode = terminal_mode

    def create_func_obj(self, func_code_str):
        g = dict()
        l = dict()
        exec(b64decode(func_code_str).decode('ascii'), g, l)
        if l: return list(l.values())[0]
        return None

    def open_yaml(self, path):
        try:
            stream = open(path, 'r')
        except FileNotFoundError as e:
            raise self.YamlIOError(e)
        except RuntimeError as e:
            raise self.YamlIOError('error reading yaml file. The message is %s' % e)
        return  yaml.load(stream, Loader=yaml.Loader)

    def plugin_imports(self):
        global utils, filters, metrics, metrics, flags
        if 'utils' in sys.modules:
            import utils
        if 'filters' in sys.modules:
            import filters
        if 'flags' in sys.modules:
            import flags
        if 'metrics' in sys.modules:
            import metrics

    def read_yaml(self, path, headers):
        #read yaml
        yaml_data = self.open_yaml(path)
        #check if all data needed is available
        try:
            self.check_data_needed_available(yaml_data, headers)
        except self.ValidationError as e:
            raise self.ValidationError(e)
        #build dicts with selected fields, metrics and flags. create feature if needed
        metrics_to_apply = []
        filters = []
        selected_features = []
        metrics_to_apply = []
        flags = []
        if 'plugins' in yaml_data.keys():
            #load plugins
            self.plugin_loader.load_plugin(yaml_data['plugins'])
            #import plugin modules
            self.plugin_imports()
        if 'filters' in yaml_data.keys():
            if self.terminal_mode: self.console.print('Parsing Filters', style='khaki3')
            try:
                filters = self.parse_filters(yaml_data['filters'])
            except self.ValidationError as e:
                raise self.ValidationError(e)
        if 'flags' in yaml_data.keys():
            if self.terminal_mode: self.console.print('Parsing Flags', style='khaki3')
            flags = self.parse_flags(yaml_data['flags'])
        if 'features' in yaml_data.keys():
            if self.terminal_mode: self.console.print('Parsing Features', style='khaki3')
            try:
                selected_features, metrics_to_apply = self.parse_features(yaml_data['features'])
            except ValidationError as e:
                raise self.ValidationError(e)
        return selected_features, metrics_to_apply, flags, filters

    #return tuple (function name, implementation)
    def get_module_implementations(self, module):
        return dict(inspect.getmembers(module, inspect.isfunction)) if module.__name__ in sys.modules else {}

    #check all features have required dataset column
    def check_data_needed_available(self, yaml_data, headers):
        sections = ['filters', 'features', 'flags']
        # for each section, add to a set every feature needed
        for s in sections:
            if s in yaml_data:
                for f in yaml_data[s]:
                    data_needed = f['data_needed'] if 'data_needed' in f.keys() else f['name']
                    for dn in data_needed.split(','):
                        if dn not in headers:
                            raise self.ValidationError('%s %s needs %s, which is not in the given dataset' % (s[:-1], f['name'], dn))
                    if 'function_code' in f.keys() and not f['function_code']:
                        raise self.ValidationError('%s %s needs %s, which is not in the given dataset' % (s[:-1], f['name'], dn))

    def validate_filter(self, _filter, implemented_filters):
        if 'name' not in _filter:
            raise self.ValidationError('Filter doesn\'t have name')
        if _filter['name'] not in implemented_filters and 'function_code' not in _filter:
            raise self.ValidationError('The filter %s was not found on the implemented filters but it\'s function code was not provided either' % _filter['name'])
        if 'data_needed' not in _filter:
            raise self.ValidationError('The filter %s was included without declaring the data needed to use it' % _filter['name'])
        if 'params' in _filter and len(_filter['params']) == 0:
            raise self.ValidationError('The filter %s declared parameters but the list is empty' % _filter['name'])

    def parse_filters(self, _filters):
        implemented_filters = self.get_module_implementations(filters) if 'filters' in sys.modules else []
        filters_to_apply = []
        for f in _filters:
            try:
                self.validate_filter(f, implemented_filters)
            except self.ValidationError as e:
                raise self.ValidationError(e)
            impl = implemented_filters[f['name']] if f['name'] in implemented_filters else self.create_func_obj(f['function_code'])
            params = f['params'] if 'params' in f.keys() else {}
            filters_to_apply += [{
                'name': f['name'],
                'fn': impl,
                'data_needed': f['data_needed'],
                'params': params
            }]
        return filters_to_apply

    def validate_feature(self, feature, implemented_metrics):
        if 'name' not in feature:
            raise self.ValidationError('A feature was included without a name.')
        if 'type' not in feature:
            raise self.ValidationError('The feature %s was included without declaring its type.' % feature['name'])
        if 'data_needed' not in feature:
            if feature['type'] != 'field':
                raise self.ValidationError('The feature %s was included without declaring the data needed to use it' % feature['name'])
        if 'multiplier' not in feature:
            raise self.ValidationError('No multiplier was specified for the feature %s' % feature['name'])
        if type(feature['multiplier']) != int:
            raise self.ValidationError('Multiplier is not integer')
        if feature['type'] == 'metric' and feature['name'] not in implemented_metrics and 'function_code' not in feature:
            raise self.ValidationError('Unknown metric %s' % feature['name'])

    def parse_features(self, features):
        selected_features = []
        metrics_to_apply = []
        implemented_metrics = self.get_module_implementations(metrics) if 'metrics' in sys.modules else []
        for f in features:
            try:
                self.validate_feature(f, implemented_metrics)
            except self.ValidationError as e:
                raise self.ValidationError(e)
            #add field from df
            if f['type'] == 'field':
                selected_features.append(f['name'])
            else: #metric or flag type
                impl=implemented_metrics[f['name']] if f['name'] in implemented_metrics else self.create_func_obj(f['function_code'])
                params = f['params'] if 'params' in f.keys() else {}
                if 'data_needed' not in f.keys():
                    f['data_needed'] = f['name']
                metrics_to_apply += [{
                    'name': f['name'],
                    'data_needed' : f['data_needed'],
                    'fn' : impl,
                    'params': params
                }]
        return selected_features, metrics_to_apply

    def validate_flag(self, flag, implemented_flags):
        if 'name' not in flag:
            raise self.ValidationError('A flag was included without a name')
        if 'description' not in flag:
            raise self.ValidationError('Missing description for flag %s' % flag['name'])
        if 'type' not in flag:
            raise self.ValidationError('Mssing type in flag')
        if flag['type'] not in ['aggregation', 'direct']:
            raise self.ValidationError('Uknown flag type %s' % flag['type'])
        if flag['type'] != 'aggregation' and 'message' not in flag:
            raise self.ValidationError('Missing message for flag %s' % flag['name'])
        if 'data_needed' not in flag:
            raise self.ValidationError('The flag %s was included without declaring the data needed to use it' % flag['name'])
        if 'severity' not in flag:
            raise self.ValidationError('Missing severity for flag %s' % flag['name'])
        if flag['severity'] not in ['low', 'medium', 'high']:
            raise self.ValidationError('Uknown severity %s' % flag['severity'])
        if flag['name'] not in implemented_flags and 'function_code' not in flag:
            raise self.ValidationError('The flag %s was not found in the implemented flags but the function code was not included neither' % flag['name'])

    def parse_flags(self, _flags):
        flags_to_apply = []
        implemented_flags = self.get_module_implementations(flags) if 'flags' in sys.modules else []
        for f in _flags:
            try:
                self.validate_flag(f, implemented_flags)
            except self.ValidationError as e:
                raise self.ValidationError(e)
            impl = implemented_flags[f['name']] if f['name'] in implemented_flags else self.create_func_obj(f['function_code'])
            params = f['params'] if 'params' in f.keys() else {}
            partial_obj = {
                'name': f['name'],
                'description': f['description'],
                'type': f['type'],
                'data_needed' : f['data_needed'],
                'severity': f['severity'],
                'fn' : impl,
                'params': params
            }
            if 'message' in f:
                partial_obj['message'] = f['message']
            flags_to_apply += [partial_obj]
        return flags_to_apply

    def apply_filters(self, df, filters):
        for f in filters:
            if len(f['params']) != 0:
                df.drop(df[f['fn'](df[f['data_needed']], f['params'])].index, inplace=True)
            else:
                df.drop(df[f['fn'](df[f['data_needed']])].index, inplace=True)

    def run_flags(self, df, flags):
        flags_results = []
        aux_result = None
        for f in flags:
            needed = f['data_needed'].split(',')
            if f['type'] == 'aggregation':
                aux_result, message = f['fn'](df, f['params'])
                f['message'] = message
                f['result'] = aux_result
            else:
                has_params = len(f['params']) != 0
                if len(needed) > 1:
                    if has_params:
                        aux_result = df[needed].apply(lambda x: f['fn'](*x, f['params']), axis=1)
                    else:
                        aux_result = df[needed].apply(lambda x: f['fn'](*x), axis=1)
                else:
                    if has_params:
                        aux_result = df[needed[0]].apply(f['fn'], f['params'])
                    else:
                        aux_result = df[needed[0]].apply(f['fn'])
                f['result'] = df.loc[aux_result]
            #we don't need the implementation anymore
            del f['fn']
            flags_results.append(f)
        return flags_results

    # Grab selected features or apply metrics
    def apply_features(self, df, selected_fields, metrics, headers):
        for f in metrics:
            needed = f['data_needed'].split(',')
            has_params = len(f['params']) != 0
            if len(needed) > 1:
                if has_params:
                    df[f['name']] = df[needed].apply(lambda x: f['fn'](*x, f['params']), axis=1)
                else:
                    df[f['name']] = df[needed].apply(lambda x: f['fn'](*x), axis=1)
            else:
                if has_params:
                    df[f['name']] = df[needed[0]].apply(f['fn'], f['params'])
                else:
                    df[f['name']] = df[needed[0]].apply(f['fn'])
        #TODO column renaming should be done here
        #erase original columns
        return df.drop(columns=list(set(headers) - set(selected_fields)), inplace=True)

    def realtime_processing(self, in_driver, selected_features, metrics, headers, collection_time):
        df = None
        timeout = time.time() + collection_time
        while time.time() < timeout:
            aux_df = in_driver.get_register()
            if aux_df is None: continue
            self.apply_features(aux_df, selected_features, metrics, headers)
            df = pd.concat([df, aux_df])
        flags_results = run_flags(df, flags)
        if len(filters) != 0: apply_filters(aux_df, filters)
        return df, flags_results

    def run(self, features_file):
        if self.terminal_mode: self.console.print('Connecting with drivers', style='khaki3')
        #obtain headers
        try:
            headers = self.in_driver.get_fields()
        except Exception as e:
            sys.exit('There was a problem reading the headers of the file')
        #read features
        if self.terminal_mode: self.console.print('Reading Yaml file', style='khaki3')
        try:
            selected_features, metrics, flags, filters = self.read_yaml(features_file, headers)
        except self.ValidationError as e:
            sys.exit(e)
        #apply features
        df = None
        if not self.in_real_time:
            #Read static data
            df_original = self.in_driver.get_data()
            #Apply filters if they exist
            if len(filters) > 0: self.apply_filters(df_original, filters)
            #Grab selected features
            if len(selected_features) == 0:
                if len(flags) > 0: flags_results = self.run_flags(df_original, flags)
            else:
                if len(flags) > 0:
                    df = df_original.copy()
                    flags_results = self.run_flags(df, flags)
                else:
                    df = df_original
                    flags_results = []
                self.apply_features(df, selected_features, metrics, headers)
        else:
            df, flags_results = self.realtime_processing(self.in_driver, selected_features, metrics, headers, self.in_driver.collection_time)
        #we have the data, we can disconnect from source
        self.in_driver.disconnect()
        if self.save_results:
            if df is not None: self.out_driver.save(df, 'behavior_analysis.csv')
            if len(flags_results) > 0:
                for f in flags_results:
                    self.out_driver.save(f['result'], f['name'] + '.csv')
            self.out_driver.disconnect()
        return df_original, df, flags_results


