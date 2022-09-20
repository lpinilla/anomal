import pytest
import os
import shutil
import sys

from rich.console import Console
from feature_engine.plugin_loader import PluginLoader
from inspect import getmembers, isfunction


console = Console()

dirname = os.path.dirname(__file__) + '/'

base_folder = dirname + 'plugins/'

plugin_repo = 'https://github.com/lpinilla/pf-test-plugin-1'

def test_download_repo():
    plugin_loader = PluginLoader(console, True, plugin_base_folder=base_folder)
    if os.path.exists(base_folder + 'pf-test-plugin-1'):
        shutil.rmtree(base_folder + 'pf-test-plugin-1')
    #download repo from github link
    plug = plugin_loader.clone_plugin_from_github(plugin_repo)
    assert plug == 'pf-test-plugin-1'
    #check folder exists
    assert os.path.exists(base_folder + 'pf-test-plugin-1')
    #remove the repo's folder
    shutil.rmtree(base_folder + 'pf-test-plugin-1')

def test_import_module():
    plugin_loader = PluginLoader(console, True, plugin_base_folder=base_folder)
    #download repo from github link
    plug = plugin_loader.clone_plugin_from_github(plugin_repo)
    assert 'filters' not in sys.modules
    assert 'flags' not in sys.modules
    plugin_loader.import_module(plug, 'filters')
    plugin_loader.import_module(plug, 'flags')
    assert 'filters' in sys.modules
    assert 'flags' in sys.modules
    expected_functions = ['cashout_great_amount', 'remove_transactions_lower_than', 'nameDest_starts_with_C']
    loaded_functions = [x[0] for x in getmembers(sys.modules['flags'], isfunction)]
    loaded_functions += [x[0] for x in getmembers(sys.modules['filters'], isfunction)]
    assert set(expected_functions) == set(loaded_functions)
    shutil.rmtree(base_folder + 'pf-test-plugin-1')

def test_install_plugin():
    plugin_loader = PluginLoader(console, True, plugin_base_folder=base_folder)
    if os.path.exists(base_folder + 'pf-test-plugin-1'):
        shutil.rmtree(base_folder + 'pf-test-plugin-1')
    #download repo from url
    plug = plugin_loader.clone_plugin_from_github(plugin_repo)
    assert os.path.exists(base_folder + 'pf-test-plugin-1')
    plugin_loader.install_plugins([plug])
    assert os.path.exists(base_folder+'installed_plugins.pkl')
    #with open('log.txt', 'r') as f: #FIXME log needs to be installed on the plugin folder, not on the main
    with open(base_folder + plug + '/log.txt', 'r') as f:
        result = f.read()[:-1] #remove new line
    assert 'setup successfully' == result
    #remove the repo's folder
    shutil.rmtree(base_folder + plug)
    os.remove(base_folder + 'installed_plugins.pkl')

