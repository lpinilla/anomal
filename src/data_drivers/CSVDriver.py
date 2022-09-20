from data_drivers.StaticDriver import InputDriver, OutputDriver
import pandas as pd

class CSVInputDriver(InputDriver):

    df = None
    path = None

    def setup(self, path):
        if not path: raise ValueError('Please provide path to dataset')
        self.path = path

    def connect(self):
        try:
            self.df = pd.read_csv(self.path, header=0, on_bad_lines='skip')
        except Exception as e:
            return e
        self.df.fillna('', inplace=True)
        self.df.index.name = 'id'

    def get_fields(self):
        if self.df.empty:
            print('Not connected to dataset')
        return list(self.df.columns)

    def get_data(self):
        if self.df.empty:
            print('Not connected to dataset')
        return self.df

    def disconnect(self):
        self.df = None
        return True

class CSVOutputDriver(OutputDriver):

    path = None
    prefix = None
    connected = None

    def setup(self, path):
        if not path:
            raise ValueError('Please provide path to dataset')
        self.path = path

    def connect(self):
        if not self.prefix:
            self.prefix = ''
        if self.path[-1] == '.':
            self.path = self.path[:-1]
        self.connected = True
        return True

    def save(self, df, name):
        df.to_csv(self.path + self.prefix + name)
        return True

    def disconnect(self):
        if self.connected:
            return True
        else:
            raise ConnectionError('Driver wasn\'t conencted')

