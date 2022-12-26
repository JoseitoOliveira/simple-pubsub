"""A simple pubsub based on socket."""

__version__ = "0.1"

import pickle
import struct
import traceback
from dataclasses import dataclass
from multiprocessing import Process
from socket import AF_INET, SOCK_STREAM, socket
from threading import Thread
from time import sleep
from typing import Any, Callable, Dict, Optional, Set, Text

UNSIGNED_INT_SIZE = 4
UNSIGNED_INT_FORMAT = '>I'

@dataclass
class Package:
    type: str
    topic: str
    message: Optional[Any] = None

def __send_bytes(conn, bytes):
    data = struct.pack(UNSIGNED_INT_FORMAT, len(bytes)) + bytes
    conn.sendall(data)


def __recv_bytes(conn):
    # Read message length and unpack it into an integer
    raw_msglen = __recvall(conn, UNSIGNED_INT_SIZE)
    if not raw_msglen:
        raise Exception('Unexpected message lenght.')
    msglen = struct.unpack(UNSIGNED_INT_FORMAT, raw_msglen)[0]
    # Read the message data
    return __recvall(conn, msglen)


def send(conn, data:Package):
    serialized_data = pickle.dumps(data)
    __send_bytes(conn, serialized_data)


def recv(conn):
    bytes = __recv_bytes(conn)
    if bytes:
        obj = pickle.loads(bytes)
        return obj


def __recvall(conn, n):
    # Helper function to recv n bytes or return None if EOF is hit
    data = bytearray()
    while len(data) < n:
        packet = conn.recv(n - len(data))
        if not packet:
            return None
        data.extend(packet)
    return data


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
        self.__run = False
        self.__topics: Dict[Text, Set[socket]] = dict()

    def init(self, with_process=False):
        self.__run = True
        if with_process:
            self.process = Process(target=self.server_loop)
        else:
            self.process = Thread(target=self.server_loop)
        self.process.start()
    
    def run_forever(self):
        while self.process.is_alive():
            sleep(0.1)
    
    def stop(self):
        self.__run = False

    def __subscribe(self, data:Package, conn: socket):
        if data.topic not in self.__topics:
            self.__topics[data.topic] = {conn}
        else:
            self.__topics[data.topic].add(conn)


    def __unsubscribe(self, data:Package, conn: socket):
        self.__topics[data.topic].remove(conn)


    def __message(self, data:Package, conn: socket):
        if data.topic in self.__topics:
            for _conn in self.__topics[data.topic]:
                send(_conn, data)
    
    def server_loop(self):
        print('Foo')
        with socket(AF_INET, SOCK_STREAM) as s:
            s.bind((self.host, self.port))
            s.listen()
            while self.__run:
                conn, addr = s.accept()
                Thread(target=self.__client_handler, args=(conn, addr)).start()
            s.detach()
            s.close()

    def __client_handler(self, conn, addr):
        funcs: Dict[Text, Callable] = {
            'message': self.__message,
            'subscribe': self.__subscribe,
            'unsubscribe': self.__unsubscribe,
        }

        with conn:
            if self.verbose:
                print('Connected by', addr)
            
            while True:
                try:
                    data = recv(conn)
                except ConnectionResetError:
                    for topic in self.__topics.keys():
                        if conn in self.__topics[topic]:
                            self.__topics[topic].remove(conn)
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
    HOST = '127.0.0.1'  # Standard loopback interface address (localhost)
    PORT = 65432        # Port to listen on (non-privileged ports are > 1023)
    server = Server(HOST, PORT)
    server.init()
    server.wait()
