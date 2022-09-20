import pandas as pd
import numpy as np
import rich
import time
import yaml
import pickle
import argparse

import data_drivers as drivers


from feature_engine.feature_engine import FeatureEngine
from classification_engines import gower_nmds_classification
from report import Report
from rich.console import Console
from rich.panel import Panel
from os import walk, system, path, environ

class Anomal():

    parser = None
    args = None
    base_folder = None
    console = None

    def __init__(self):
        self.parser = self.create_parser()
        self.args = vars(self.parser.parse_args())
        self.console = Console()

    def get_console(self):
        return self.console

    def create_parser(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--base-folder', help='Specify the base folder to work')
        parser.add_argument('-c', '--config-file', help='Specify the system config file that will be used to configure the system')
        parser.add_argument('-ce', '--class-engine', nargs='?', help='Specify the classification engine that will be used by the system', default='Gower')
        parser.add_argument('-di', '--driver-in', help='Specify the driver to be used for the input')
        parser.add_argument('-do', '--driver-out', help='Specify the driver to be used for the output')
        parser.add_argument('-in', '--input-file', help='Specify name of the input that inside the base folder')
        parser.add_argument('-out', '--output-file', help='Specify the name of the output data inside the base folder')
        parser.add_argument('-ff', '--features-file', help='Specify the name of the file in the base folder that has the features')
        parser.add_argument('--collection-time', help='Specify the overall time to collect data in real time drivers', type=float)
        parser.add_argument('--tcp-in-host', help='Specify the host of a real time driver')
        parser.add_argument('--tcp-in-port', help='Specify the port of a real time driver', type=int)
        parser.add_argument('-sF', help='Save feature engine result', action='store_true')
        parser.add_argument('-sC', help='Save classification engine result', action='store_true')
        parser.add_argument('-sFC', help='Save both feature and classification engines result', action='store_true')
        parser.add_argument('-p', '--report-port', help='Select the port on which the report will run', nargs='?', type=int, default=8080)
        parser.add_argument('--no-ui', help='Use the system without the tui', action='store_true')
        return parser

    def arg_check(self, param, msg):
        if not param:
            print(msg)
            parser.print_help()
            exit(0)

    def check_arguments(self):
        self.arg_check(self.args['base_folder'], 'No base folder specified')
        self.base_folder = self.args['base_folder'] + '/' if self.args['base_folder'][-1] != '/' else self.args['base_folder']
        if not self.args['config_file']:
            #no config file, check every argument
            self.arg_check(self.args['driver_in'], 'No input driver specified')
            self.arg_check(self.args['driver_out'], 'No output driver specified')
            self.arg_check(self.args['input_file'], 'No data input path specified')
            self.arg_check(self.args['output_file'], 'No data output path specified')
            self.arg_check(self.args['features_file'], 'No path will features specified')
        else:
            self.arg_check(path.exists(self.base_folder + self.args['config_file']), 'System config file not found')

    def extract_drivers_from_file(self, config):
        return config['setup']['input_driver'], config['setup']['output_driver']

    def extract_paths(self, config):
        if config != None:
            ret = None
            try:
                ret = config['data']['in_path'].split('/')[-1],  \
                      config['data']['out_path'].split('/')[-1], \
                      config['features']['path'].split('/')[-1]
            except Exception as e:
                exit('Something went wrong reading the config file, check it is ok')
        else:
            ret = self.args['input_file'], self.args['output_file'], self.args['features_file']
        return ret

    def get_data_drivers(self, config, base_folder):
        if config != None:
            #parse drivers from sys config file
            in_driver_name, out_driver_name = self.extract_drivers_from_file(config)
        else:
            #parse drivers from arguments
            in_driver_name = self.args['driver_in'].upper()
            out_driver_name = self.args['driver_out'].upper()
        in_path, out_path, features_file = self.extract_paths(config)
        #get driver classes from input
        (in_driver, in_real_time) = self.get_driver_instance(in_driver_name, True)
        (out_driver, out_real_time) = self.get_driver_instance(out_driver_name, False)
        #setup drivers
        in_driver, out_driver = self.setup_drivers(config,base_folder,in_driver,in_driver_name,out_driver,out_driver_name)
        return in_driver, in_real_time, out_driver, out_real_time

    def get_driver_instance(self, arg, input_type):
        if arg == 'CSV':
            if input_type:
                return drivers.CSVDriver.CSVInputDriver(), False
            else:
                return drivers.CSVDriver.CSVOutputDriver(), False
        elif arg == 'TCP':
            if input_type:
                return drivers.TCPDriver.TCPInputDriver(), True
        elif arg == 'TERMINAL':
            if not input_type:
                return drivers.TerminalDriver.TerminalOutputDriver(), False
        else:
            return None

    def setup_drivers(self, config, base_folder, in_driver, in_driver_name, out_driver, out_driver_name):
        try:
            if in_driver_name == 'CSV':
                in_driver.setup(base_folder + self.args['input_file'] if config == None else base_folder + config['data']['in_path'])
            elif in_driver_name == 'TCP':
                if config == None:
                    self.arg_check(self.args['collection-time'], 'Collection Time not provided')
                    self.arg_check(self.args['collection_time'] > 0, 'Collection Time lower than zero')
                    self.arg_check(self.args['tcp_in_host'], 'TCP host not provided')
                    self.arg_check(self.args['tcp_in_port'], 'TCP port not provided')
                    self.arg_check(self.args['tcp_in_port'] > 0, 'TCP port has to be greater than 0')
                    self.arg_check(self.args['tcp_in_port'] < 65536, 'TCP port has to be lower than 65536')
                    in_driver.setup(self.args['tcp_in_host'], self.args['tcp_in_port'], self.args['--collection-time'])
                else:
                    in_driver.setup(config['data']['host'], int(config['data']['port']), float(config['data']['collection_time']))
        except Exception as e:
            exit('Something went wrong reading input driver from config file, check it again please')
        try:
            if out_driver_name == 'CSV':
                out_path = base_folder + self.args['output_file'] if config == None else config['data']['out_path']
                out_driver.setup(out_path)
        except Exception as e:
            exit('Something went wrong with the config file, check it again please')
        #returning drivers ready to use
        try:
            in_driver.connect()
        except Exception as e:
            exit("Something failed when trying to connect to the input database. Please check again")
        try:
            out_driver.connect()
        except Exception as e:
            exit("Something failed when trying to connect to the output database. Please check again")
        return in_driver, out_driver

    def get_classification_engine_instance(self, engine):
        if engine == 'Gower':
            return gower_nmds_classification.GowerNMDS()

    def open_yaml(self, path):
        try:
            stream = open(path, 'r')
        except RuntimeError:
            print('error reading yaml file')
        return  yaml.load(stream, Loader=yaml.Loader)

    def parse_features_weights(self, base_folder, features_file):
        weights = []
        features = self.open_yaml(base_folder + features_file)['features']
        if features:
            for f in features:
                weights.append(f['multiplier'])
            return np.array(weights)
        return None

    def get_classification_engine(self, base_folder, config, df):
        #get name of engine
        if config != None:
            engine_name = config['setup']['classification_engine']
            weights_param = config['features']['path']
        else:
            engine_name = self.args['class_engine']
            weights_param = self.args['features_file']
        classification_engine = self.get_classification_engine_instance(engine_name)
        weights = self.parse_features_weights(base_folder, weights_param)
        #setup the engine
        classification_engine.setup(df, weights)
        return classification_engine

    def print_banner(self):
        logo = '''
                                                                                                                            
                                                                              ,***********,                                 
                                                                           *******************                              
                                                                         *******/&,&****%& %****                            
                                                                       ********&&*,,,,,,, ,&&****.                          
                                                                       *******#&*,,,,,,   ,*&&****                          
                                                                      *********&&,,,,,    .(&(****,                         
                                                                      **********&&/,,,,,,*&&/*****                          
                                                                       ******&((&*/&&&&&&#********                          
                                                                        ***&@@@******************                           
                                                                          &&%******************                             
                                                                            .***************                                
                                                                                  ....                                      
                                                                                                                            
                                                           *****                                             &&&            
                                                           ** **   *******    *****.  &&.&&& &&&&   &&&&&&   &&&            
                                                          *** ***  ***   **  ***  *** &&&  &&  &&&       &&  &&&            
                                                          ***  **, ***   **  **   *** &&&  &&   &&  &&&&&&&  &&&            
                                                         ********* ***   **  ***  *** &&&  &&   &&  &&   &&  &&&            
                                                        **      ** ***   **    ****   &&&  &&   &&  &&&&&&&  &&&            
                                                                                                                            
                                                                   The more you know... the more you find                   
                                                                                                                            
                                                                        Made by Lautaro Pinilla                             
                                                                        Github: lpinilla/anomal                             
                                                                                                                            
                                                                                                                            
                                                                                                                            
    '''
        self.console.print(Panel(logo, subtitle='This is the terminal client for the \'Anomal\' anomaly detection framework'))

    def run(self):
        self.check_arguments()
        #get sys variables
        system_config_file = self.args['config_file']
        #read config file
        if self.args['config_file']:
            self.console.print('Reading Config File', style='sky_blue1')
            config = self.open_yaml(self.base_folder + system_config_file)
        else:
            self.console.print('Parsing Arguments', style='sky_blue1')
            config = None
        in_driver, in_real_time, out_driver, out_real_time = self.get_data_drivers(config, self.base_folder)
        #get paths from config
        features_file = self.extract_paths(config)[2]
        #instantiate feature engine
        self.console.print('Starting Feature Engine', style='khaki3')
        feature_engine = FeatureEngine(in_driver, out_driver, in_real_time, self.base_folder + features_file, self.args['sF'] or self.args['sFC'], self.console, True)
        #run feature engine, get new dataframe
        df_original, df, flags_results = feature_engine.run(self.base_folder + features_file)
        #Start Classification Engine
        self.console.print('Starting Classification Engine', style='indian_red')
        #Get already started classification engine
        if df is not None:
            classification_engine = self.get_classification_engine(self.base_folder, config, df)
            #Save classification engine's results
            if self.args['sC'] or self.args['sFC']:
                out_driver.save(classification_engine.matrix_to_dataframe, 'classification_result.csv')
        else:
            self.console.print('No features found, skipping Classification Engine')
            classification_engine = None
        self.console.print('Creating Report', style='spring_green2')
        report = Report(df, df_original, classification_engine, flags_results)
        self.console.print('Creating diagrams', style='spring_green2')
        if df is not None: report.bake_data()
        self.console.print('Report available at localhost:%d'% self.args['report_port'], style='spring_green2')
        environ['PYWEBIO_SCRIPT_MODE_PORT'] = str(self.args['report_port'])
        report.start()
