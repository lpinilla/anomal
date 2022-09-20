import importlib.util
import pkgutil
import sys
import os
import subprocess
import pickle
from git import Repo

from rich.console import Console

class PluginLoader():

    plugin_base_folder = 'feature_engine/plugins/'
    installed_plugins = None
    available_plugins = None
    console = None
    terminal_mode = None

    def __init__(self, console, terminal_mode, plugin_base_folder = 'feature_engine/plugins/'):
        self.plugin_base_folder = plugin_base_folder
        try:
            self.available_plugins = next(os.walk(self.plugin_base_folder))[1]
        except StopIteration:
            #no available plugins installed
            self.available_plugins = []
            self.installed_plugins = []
            print('error fetching available plugins, will continue anyway')
        if os.path.exists(self.plugin_base_folder + 'installed_plugins.pkl'):
            self.installed_plugins = pickle.load(open(self.plugin_base_folder + 'installed_plugins.pkl', 'rb'))
        else:
            self.installed_plugins = []
        self.console = console
        self.terminal_mode = terminal_mode

    def clone_plugin_from_github(self, url):
        plugin_name = url.split('/')[-1]
        Repo.clone_from(url, self.plugin_base_folder + plugin_name)
        self.available_plugins += [plugin_name]
        return plugin_name

    def import_module(self, plugin, module_name):
        plugin_path = self.plugin_base_folder + plugin + '/' + module_name + '.py'
        if not os.path.exists(plugin_path): return
        module_spec = importlib.util.spec_from_file_location(module_name, plugin_path)
        module = importlib.util.module_from_spec(module_spec)
        module_spec.loader.exec_module(module)
        #This would allow multiple plugins. To do this, we need to import
        #each module separately, build a base module and load every
        #method into this module
        sys.modules[module_name] = module

    #install a plugin that was already downloaded
    def install_plugins(self, available_plugins):
        for plugin in available_plugins:
            if plugin not in self.installed_plugins:
                setup_file = self.plugin_base_folder + plugin + '/setup.sh'
                print(setup_file)
                if os.path.exists(setup_file):
                    if self.terminal_mode: self.console.print('Running setup.sh for Plugin %s' % plugin, style='steel_blue')
                    install_process = subprocess.run(['bash', '+x' , 'setup.sh'], cwd=self.plugin_base_folder + plugin)
                    self.installed_plugins += [plugin]
                    pickle.dump(self.installed_plugins, open(self.plugin_base_folder + 'installed_plugins.pkl', 'wb'))
                else:
                    if self.terminal_mode: self.console.print('No setup.sh found for Plugin %s' % plugin, style='steel_blue')

    def import_plugin_modules(self, plugin):
        # there may be dependencies between one module and another so first load utils
        self.import_module(plugin, 'utils')
        self.import_module(plugin, 'filters')
        self.import_module(plugin, 'flags')
        self.import_module(plugin, 'metrics')

    def load_plugin(self, requested_plugin):
        if self.terminal_mode: self.console.print('Loading Plugin %s' % requested_plugin, style='steel_blue')
        #requesting a plugin from github, clone repo and install it
        if requested_plugin not in self.available_plugins:
            if 'github.com' in requested_plugin:
                url = requested_plugin
                requested_plugin = url.split('/')[-1]
                if requested_plugin not in self.available_plugins:
                    if self.terminal_mode: self.console.print('Cloning Plugin from github', style='steel_blue')
                    requested_plugin = self.clone_plugin_from_github(url)
                    if self.terminal_mode: self.console.print('Installing Plugin', style='steel_blue')
                    self.install_plugins([requested_plugin])
            else:
                sys.exit('The requested plugin was not found in the /plugins folder')
        if self.terminal_mode: self.console.print('Importing Plugin\'s modules', style='steel_blue')
        self.import_plugin_modules(requested_plugin)
        if self.terminal_mode: self.console.print('Plugin %s imported successfully' % requested_plugin, style='steel_blue')

    def load_plugins(self):
        #get all available plugins in the 'plugins/' folder
        self.install_plugins(self.available_plugins)
        for plugin in self.available_plugins:
            self.import_plugin_modules(plugin)
