import tkinter as tk
import yaml
import rich

from anomal import Anomal
from feature_engine.feature_engine import FeatureEngine
from classification_engines import gower_nmds_classification
from report import Report


from os import walk, system, name, environ, path
from rich.progress import Progress, BarColumn
from rich.markdown import Markdown
from rich.prompt import Prompt, Confirm
from rich.panel import Panel
from rich.columns import Columns
from rich.padding import Padding
from rich.layout import Layout
from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich.syntax import Syntax
from rich.tree import Tree

#clear screen
system('cls' if name == 'nt' else 'clear')

anomal = Anomal()
anomal.print_banner()
console = anomal.get_console()

if anomal.args['no_ui']:
    anomal.run()
    exit(0)

print('\n\n\n\n\n')
base_folder = Prompt.ask("Select base folder", default='/examples')
if base_folder[-1] != '/':
    base_folder += '/'
if not path.exists(base_folder): exit('Base folder not found')

#------ SETUP

#Build progress bar and layout
progress = rich.progress.Progress(
    "[progress.description]{task.description}",
    BarColumn(),
    "[progress.percentage]{task.percentage:>3.0f}%",
)
setup_progress = progress.add_task('[cyan]Setup...', total=10)
progress.stop()
layout = Layout()
layout.split_column( Layout(name="upper"), Layout(name="lower"))
layout['lower'].split_row( Layout(name='lower_left'), Layout(name='lower_right'))
progress_table = Table.grid(expand=True)
progress_table.add_row() #for styling
progress_table.add_row(Panel(progress, subtitle='Progress', border_style='green', padding=(0,55)))
layout['upper'].size = 4
layout['upper'].update(progress_table)

def step_progress():
    progress.update(setup_progress, advance=1)

def option_layout(options, description):
    panels_layout(Columns([Panel(x) for x in options]),description)

def panels_layout(renderable, description, left_title='Options'):
    layout['lower_right'].update(Panel(Markdown(description), title='Description', border_style='blue'))
    if renderable is not None:
        layout['lower_left'].update(Panel(renderable, title=left_title, padding=(3,20)))
    else:
        layout['lower_left'].update('')
    console.print(layout)

def big_panel_layout(renderable, description,left_title='Options'):
    layout['lower_right'].update(Panel(Markdown(description), title='Description', border_style='blue'))
    if renderable is not None:
        layout['lower_left'].update(Panel(renderable, title=left_title))
    else:
        layout['lower_left'].update('')
    console.print(layout)

#------- Has Config File
def menu_has_config_file():
    has_conf_expl = '''
## Skip Config file Wizard?

If you already have a configuration file for the Feature Engine,
you could skip the wizard and move the steps for creating one and
run the engines.
    '''
    left_menu = ''
    panels_layout(left_menu, has_conf_expl, left_title='')
    return Confirm.ask('Do you have a feature config file?')

#--------- Load Config File

def menu_load_config_file():
    features_config = '''
## System Config File

Please select the yaml file that contains the properties for the system.
    '''
    feature_panel = Tree(base_folder)
    for f in next(walk(base_folder))[2]:
        feature_panel.add(f)
    found_file = True
    while True:
        panels_layout(feature_panel, features_config, left_title='File Selection')
        if not found_file:
            console.print('File not found!')
        system_config_file = Prompt.ask("Enter name for the config file :file_folder:")
        if not path.exists(base_folder + system_config_file):
            found_file = False
        else:
            break
    for i in range(0, 5): #skip steps
        step_progress()
    return system_config_file


#------- IN Driver
def menu_in_input():
    data_in_expl = '''
## Please select a data driver for the input data.

Data drivers are ways of communicating to data sources.

There are two types of data drivers, `static` and `real time`.

## Static Drivers

Static Drivers are meant for data sources which will not change.
Meanining that the data is on one place and it will not be updated
while accessing it.

## Real Time Drivers

Real time drivers are meant for data souces that will change over time.
This could be useful on cases where your data comes a logging service and
is not stored on one place but read as served.
    '''
    in_options = ['CSV', 'TCP (Real Time)']
    option_layout(in_options, data_in_expl)
    in_driver = Prompt.ask("Select input driver :electric_plug:", choices=['CSV', 'TCP'], default="CSV")
    step_progress()
    return in_driver

#------- OUT Driver

def menu_out_input():
    data_out_expl = '''
## Please select a data driver for to output the data.

Output data drivers are ways of storing the data.

The data is stored once it's done processing.

These drivers could lead either to the terminal,
a local file or send them to a database.
    '''
    out_options = ['CSV', 'Terminal', 'ElasticSearch']
    option_layout(out_options, data_out_expl)
    out_driver =  Prompt.ask("Select output driver :electric_plug:", choices=out_options, default="CSV")
    step_progress()
    return out_driver


def menu_in_driver(in_driver):
    #Build panels
    if in_driver == 'CSV':
        input_driver_expl = '''
## CSV Input Driver

Please choose the csv file with the data.
'''
        in_driver_panel = Tree(base_folder)
        for f in next(walk(base_folder))[2]:
            in_driver_panel.add(f)
    else:
        input_driver_expl = '''
## CSV Input Driver

Please enter host and port where the system will listen for data.

Also, you need to provide \'collection time\'. This is the frequency
in which we would pull the data from the source. Default value is 60s.
        '''
        in_driver_panel = ''

    #ask for input
    if in_driver == 'CSV':
        file_found = True
        while True:
            panels_layout(in_driver_panel, input_driver_expl, left_title='Parameters')
            if not file_found:
                console.print('File not found!')
            in_path = Prompt.ask("Enter IN data filename :file_folder:")
            if path.exists(base_folder + in_path):
                console.print(in_path + ':heavy_check_mark:')
                break
            else:
                file_found = False
        step_progress()
        return in_path
    else:
        panels_layout(in_driver_panel, input_driver_expl, left_title='Parameters')
        driver_info = '''
- Host: %s
- Port: %s
- Collection Time: %s
        '''
        panels_layout(driver_info % ('', '', ''), input_driver_expl, left_title='Parameters')
        in_host = Prompt.ask("Enter host", default="localhost")
        panels_layout(driver_info % (in_host, '', ''), input_driver_expl, left_title='Parameters')
        in_port = int(Prompt.ask("Enter port", default="4141"))
        panels_layout(driver_info % (in_host, str(in_port), ''), input_driver_expl, left_title='Parameters')
        in_collection_time = float(Prompt.ask("Enter collection time", default="60"))
        panels_layout(driver_info % (in_host, str(in_port), str(in_collection_time)), input_driver_expl, left_title='Parameters')
        step_progress()
        return (in_host, in_port, in_collection_time)

## -------------- OUTPUT DRIVER

def menu_out_driver(out_driver):
    if out_driver == 'CSV':
        output_driver_panel = Tree(base_folder)
        for f in next(walk(base_folder))[2]:
            output_driver_panel.add(f)
        output_driver_expl = '''
## CSV Output Driver

Please provide the path to where the data will be saved.
    '''
    elif out_driver == 'ElasticSearch':
        driver_info = '''
- Host: %s
- Port: %s
        '''
        output_driver_expl = '''
## ElasticSearch Output Driver

Please provide the host and the port to connect to the database.
        '''
        output_driver_panel = ''
    if out_driver == 'CSV':
        panels_layout(output_driver_panel, output_driver_expl, left_title='Parameters')
        out_path = Prompt.ask("Enter OUT data filename :file_folder:", default='results.csv') #check if file exists
        step_progress()
        return out_path
    elif out_driver == 'ElasticSearch':
        panels_layout(driver_info % ('', ''), output_driver_expl, left_title='Parameters')
        out_host = Prompt.ask("Enter host", default="localhost")
        panels_layout(driver_info % (out_host, ''), output_driver_expl, left_title='Parameters')
        out_port = int(Prompt.ask("Enter port", default="4141"))
        panels_layout(driver_info % (out_host, str(out_port)), output_driver_expl, left_title='Parameters')
        return (out_host, out_port)
        step_progress()
#------- Features

def menu_features():
    features_expl = '''
## Features File

Please select the yaml file that contains the features to be used by
the system.

Each feature must contain:

- The data needed for that feature
- It's name if it is a builtin function of a library (for example, dnslib)
- It's base64-encoded-implementation if it doesn't belong to a feature library.
    '''
    feature_panel = Tree(base_folder)
    for f in next(walk(base_folder))[2]:
        feature_panel.add(f)
    found_file = True
    while True:
        panels_layout(feature_panel, features_expl, left_title='File Selection')
        if not found_file:
            console.print('File not found!')
        features_file = Prompt.ask("Enter name for the features file :file_folder:")
        if not path.exists(base_folder + features_file):
            found_file = False
        else:
            break
    step_progress()
    return features_file
##-------- Features config file

def menu_configs_summary(loaded, in_driver, out_driver, in_path, in_host, in_port, out_path, features_file, system_config_file):
    config = { 'setup': {}, 'data': {}, 'features': {}}
    if not loaded:
        #reverse order to print it better
        #output
        if out_driver == 'CSV':
            config['data']['out_path'] = out_path
        config['features']['path'] = features_file
        #input
        if in_driver == 'CSV':
            config['data']['in_path'] = in_path
        elif in_driver == 'TCP':
            config['data']['host'] = in_host
            config['data']['port'] = in_port
        config['setup']['input_driver'] = in_driver
        config['setup']['output_driver'] = out_driver
        #hardcoded for now, it should be a separate menu but I won't make it for
        #only one option. The user can specify another engine via command line
        config['setup']['classification_engine'] = 'Gower'
        # save to file
        with open(base_folder + 'system_configs.yaml', 'w') as f:
            yaml.dump(config, f, default_flow_style=False)
        system_config_expl = '''
## System configuration file

File %s was saved to be used by the System.
    ''' % out_path
        panels_layout(Syntax(yaml.dump(config), 'yaml'), system_config_expl, left_title='Preview')
    else:
        system_config_expl = '''
## System configuration file

File %s was supplied by user.

Please confirm is the data is ok before proceeding.
    ''' % system_config_file
        with open(base_folder + system_config_file, 'r') as f:
            panels_layout(Syntax(f.read(), 'yaml'), system_config_expl, left_title='Preview')
    step_progress()
    return config, Confirm.ask('Ok?')

def menu_features_file(features_file):
    feature_file_expl = '''
## Feature File

Please review if the feature file is ok before proceeding.
    '''
    with open(base_folder + features_file, 'r') as f:
        big_panel_layout(Syntax(f.read(), 'yaml'), feature_file_expl, left_title='Preview')
    step_progress()
    return Confirm.ask('Ok?')


#------------------- Configs done, now running engines

df = None
df_original = None
flags_results = None
classification_engine = None

#-------------------------- FEATURE ENGINE

def menu_feature_engine(base_folder, config):
    global df, df_original, flags_results
    feature_engine_expl = '''
## Feature Engine

Given the dataset provided and the features, in this next step it's time to transform
the dataset into a \'feature dataset\'.

This \'feature dataset\' is made of the outputs of the features functions, which in turn
uses the original dataset's variables as input.

The Classification Engine will run using this resulting dataset.
    '''
    panels_layout('', feature_engine_expl, '')
    feature_start = Confirm.ask('Start feature engine?')
    if feature_start:
        save = Confirm.ask('Save results?')
        in_driver, in_real_time, out_driver, out_real_time = anomal.get_data_drivers(config, base_folder)
        in_path, out_path, features_file = anomal.extract_paths(config)
        feature_engine = FeatureEngine(in_driver, out_driver, in_real_time, features_file, save,console, False)
        df_original, df, flags_results = feature_engine.run(base_folder + features_file)
    else:
        print('Ignoring next steps')
        exit(0)
    step_progress()
    return feature_start

#-------------------------- CLASSIFICATION ENGINE

def menu_classification_engine(base_folder, config):
    global df, classification_engine
    classification_expl = '''
## Classification Engine

The Classification Engine uses the result of the Feature Engine to run a Machine Learning algorithm.

This engine will normalize the data and add a label to each entry.

By default, the method used uses the Gower metric.
    '''
    panels_layout('', classification_expl, '')
    classification_start = Confirm.ask('Start classification engine?')
    if df is not None:
        if classification_start:
            save = Confirm.ask('Save results?')
            classification_engine = get_classification_engine(base_folder, config, df)
        else:
            print('Ignoring step')
    else:
        console.print('No features found, skipping Classification Engine')
        classification_engine = None
    step_progress()
    return classification_start

def menu_build_report():
    global df, df_original, classification_engine, flags_results
    report_expl = '''
## Report

You can build a report to see the results of the framework with some
graphs to assists you.

If you don't want to preview the report now, it can be saved for later.
    '''
    panels_layout('', report_expl, 'Description')
    if Confirm.ask('View report?'):
        environ['PYWEBIO_SCRIPT_MODE_PORT'] = int(Prompt.ask("Enter the port number of the report", default="8080"))
        report = Report(df, df_original, classification_engine, flags_results)
        console.print('Creating diagrams', style='spring_green2')
        if df is not None: report.bake_data()
        console.print('Report available at localhost:%d'%int(environ.get("PYWEBIO_SCRIPT_MODE_PORT")), style='spring_green2')
        report.start()
        step_progress()
    else:
        step_progress()

def menu_final_screen():
    thanks = '''
# Thank you for using Anomal framework!

### For any doubts or improvements, go to
github.com/lpinilla/anomal
    '''
    disclaimer = '''
## Disclaimer

The information obtained from this framework is not absolute.

There is **no guarantee** that the label \'0\' is good and the label \'1\' is bad.

The results should be examined by an expert.
    '''
    panels_layout(Markdown(thanks), disclaimer, 'Disclaimer')

# ui start
loop = True
config = None
while loop:
    loaded = menu_has_config_file()
    if loaded:
        system_config_file = menu_load_config_file()
        if menu_configs_summary(loaded, '', '', '', '', '', '', '', system_config_file):
            loop = False
        else:
            progress.update(setup_progress, advance=-6)
        config = anomal.open_yaml(base_folder + system_config_file)
        in_driver, out_driver = anomal.extract_drivers_from_file(config)
        in_path, out_path, features_file = anomal.extract_paths(config)
    else:
        in_driver = menu_in_input()
        out_driver = menu_out_input()
        if in_driver == 'CSV':
            in_path = menu_in_driver(in_driver)
            in_host = ''
            in_port = ''
            in_collection_time = ''
        elif in_driver == 'TCP':
            (in_host, in_port, in_collection_time) = menu_in_driver(in_driver)
            in_path = ''
        if out_driver == 'CSV':
            out_path = menu_out_driver(out_driver)
            out_host = ''
            out_port = ''
        elif out_driver == 'Elasticsearch':
            (out_host, out_port) = menu_out_driver(out_driver)
            out_path = ''
        features_file = menu_features()
        config, summary_ok = menu_configs_summary(loaded,in_driver,out_driver,in_path,in_host,in_port,out_path,features_file,None)
        if summary_ok:
            loop = False
        else:
            progress.update(setup_progress, advance=-7)
if menu_features_file(features_file):
    if menu_feature_engine(base_folder, config):
        if menu_classification_engine(base_folder, config):
            menu_build_report()
    menu_final_screen()
exit(0)
