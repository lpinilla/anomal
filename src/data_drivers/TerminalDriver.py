from data_drivers.StaticDriver import InputDriver, OutputDriver

class TerminalOutputDriver(OutputDriver):

    def connect(self):
        return True

    def save(self, df):
        print(df)
        return True

    def disconnect(self):
        return True


