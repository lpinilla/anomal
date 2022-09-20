from abc import ABC, abstractmethod

class InputDriver(ABC):

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def get_fields(self):
        pass

    @abstractmethod
    def get_register(self):
        pass

    @abstractmethod
    def disconnect(self):
        pass


class OutputDriver(ABC):

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def put_register(self):
        pass

    @abstractmethod
    def disconnect(self):
        pass

