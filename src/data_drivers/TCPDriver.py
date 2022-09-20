import socket
import json
import threading
import time
from socketserver import BaseRequestHandler, ThreadingMixIn, TCPServer
from data_drivers.RealTimeDriver import InputDriver, OutputDriver
import pandas as pd

class TCPInputDriver(InputDriver):

    cache = []
    server = None
    server_thread = None
    headers = None
    host = ''
    port = 0
    counter = 0
    collection_time = 3600

    class ThreadedTCPRequestHandler(BaseRequestHandler):

        buffer_size = 4096

        def handle(self):
            data = self.request.recv(self.buffer_size)
            cur_thread = threading.current_thread()
            if len(data) > 0:
                jdata = json.loads(data.decode('utf-8'))
                TCPInputDriver.cache += [jdata]

    class ThreadedTCPServer(ThreadingMixIn, TCPServer):
        pass

    def __init__(self):
        self.cache.clear()

    def start_server(self):
        server = self.ThreadedTCPServer((self.host, self.port), self.ThreadedTCPRequestHandler)
        self.server_thread = threading.Thread(target=server.serve_forever)
        self.server_thread.daemon = True
        self.server_thread.start()
        return server

    def setup(self, host, port, collection_time):
        if not host:
            raise ValueError('Please provide a valid host')
        if port < 0:
            raise ValueError('Please provide a valid port number')
        if collection_time < 0:
            raise ValueError('Please provide a valid collection time')
        self.host = host
        self.port = port
        self.collection_time = collection_time

    def connect(self):
        self.server = self.start_server()
        return True

    def get_fields(self):
        while len(self.cache) == 0:
            time.sleep(1)
        self.headers = list(self.cache[0].keys())
        return self.headers

    def get_register(self):
        if len(self.cache) == 0: return None
        aux = self.cache.pop(0)
        aux = pd.DataFrame(index=[self.counter], data=[aux])
        self.counter+=1
        return aux

    def disconnect(self):
        self.server.shutdown()
        self.cache = []
        return True

    def __str__(self):
        return ','.join([str(x) for x in [self.host, self.port, self.collection_time, self.counter, self.cache, self.server, self.server_thread, self.headers]])
