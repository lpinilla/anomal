from abc import ABC, abstractmethod

class InputDriver(ABC):

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def get_fields(self):
        pass

    @abstractmethod
    def get_data(self):
        pass

    @abstractmethod
    def disconnect(self):
        pass

class OutputDriver(ABC):

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def save(self):
        pass

    @abstractmethod
    def disconnect(self):
        pass

