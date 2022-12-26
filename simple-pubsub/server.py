import traceback
from dataclasses import dataclass
from multiprocessing import Process
from socket import AF_INET, SOCK_STREAM, socket
from threading import Thread
from typing import Callable, Dict, Set, Text, Optional, Any

from socket_tools import recv, send

HOST = '127.0.0.1'  # Standard loopback interface address (localhost)
PORT = 65432        # Port to listen on (non-privileged ports are > 1023)

@dataclass
class Package:
    type: str
    topic: str
    message: Optional[Any] = None

class Client:
    def __init__(self, host, port) -> None:
        self.conn = socket(AF_INET, SOCK_STREAM)
        self.host = host
        self.port = port
        self.conn.connect((self.host, self.port))

    def reconnect(self):
        self.conn.connect((self.host, self.port))

    def subscribe(self, topic: Text) -> None:
        data = Package('subscribe', topic)
        try:
            return send(self.conn, data)
        except ConnectionResetError:
            self.reconnect()
            send(self.conn, data)

    def unsubscribe(self, topic: Text) -> None:
        data = Package('unsubscribe', topic)
        try:
            return send(self.conn, data)
        except ConnectionResetError:
            self.reconnect()
            send(self.conn, data)

    def send(self, topic: Text, message: object) -> None:
        data = Package('message', topic, message)
        try:
            return send(self.conn, data)
        except ConnectionResetError:
            self.reconnect()
            send(self.conn, data)

    def receive(self) -> Package:
        return recv(self.conn)


class Server:
    def __init__(self, host, port, verbose=True) -> None:
        self.host = host
        self.port = port
        self.verbose = verbose
        self.topics: Dict[Text, Set[socket]] = dict()

    def init(self):
        def broker_loop():
            with socket(AF_INET, SOCK_STREAM) as s:
                s.bind((self.host, self.port))
                s.listen()
                while True:
                    conn, addr = s.accept()
                    Thread(target=self.new_client, args=(conn, addr)).start()

        self.process = Process(target=broker_loop)
        self.process.start()
    
    def wait(self):
        while self.process.is_alive():
            pass

    def subscribe(self, data, conn: socket):
        if data['topic'] not in self.topics:
            self.topics[data['topic']] = {conn}
        else:
            self.topics[data['topic']].add(conn)


    def unsubscribe(self, data, conn: socket):
        self.topics[data['topic']].remove(conn)


    def message(self, data, conn: socket):
        if data['topic'] in self.topics:
            for _conn in self.topics[data['topic']]:
                send(_conn, data)


    def new_client(self, conn, addr):
        funcs: Dict[Text, Callable] = {
            'message': self.message,
            'subscribe': self.subscribe,
            'unsubscribe': self.unsubscribe,
        }

        with conn:
            if self.verbose:
                print('Connected by', addr)
            
            while True:
                try:
                    data = recv(conn)
                except ConnectionResetError:
                    for topic in self.topics.keys():
                        if conn in self.topics[topic]:
                            self.topics[topic].remove(conn)
                    break
            
                try:
                    if self.verbose:
                        print(data)
                    
                    _type = data['type']
                    funcs[_type](data, conn)
                except Exception:
                    print(traceback.format_exc())

        print('Desconnect by', addr)


if __name__ == "__main__":
    server = Server(HOST, PORT)
    server.wait()
