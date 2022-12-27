"""A simple pubsub based on socket."""

__version__ = "0.2"

import pickle
import struct
import traceback
from dataclasses import dataclass
from multiprocessing import Process
from socket import AF_INET, SOCK_STREAM, socket
from threading import Thread
from time import sleep
from typing import Any, Callable, Dict, Optional, Set, Text

from logol import get_logger

UNSIGNED_INT_SIZE = 4
UNSIGNED_INT_FORMAT = '>I'

log = get_logger('simple_pubsub')


@dataclass
class Package:
    type: str
    topic: Optional[str] = None
    message: Optional[Any] = None


class NonCompletedPackageReceivedError(Exception):
    ...


def __send_bytes(conn: socket, bytes):
    data = struct.pack(UNSIGNED_INT_FORMAT, len(bytes)) + bytes
    conn.sendall(data)


def __recv_bytes(conn: socket):
    # Read message length and unpack it into an integer
    raw_msglen = __recvall(conn, UNSIGNED_INT_SIZE)
    if not raw_msglen:
        raise NonCompletedPackageReceivedError
    msglen = struct.unpack(UNSIGNED_INT_FORMAT, raw_msglen)[0]
    # Read the message data
    return __recvall(conn, msglen)


def send(conn: socket, data: Package):
    serialized_data = pickle.dumps(data)
    __send_bytes(conn, serialized_data)


def recv(conn: socket):
    bytes = __recv_bytes(conn)
    if bytes:
        obj = pickle.loads(bytes)
        return obj


def __recvall(conn: socket, n):
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
        self.conn.settimeout(5)
        self.host = host
        self.port = port
        self.conn.connect((self.host, self.port))

    def reconnect(self):
        self.conn.connect((self.host, self.port))

    def send_and_retry(self, data: Package):
        try:
            return send(self.conn, data)
        except ConnectionResetError:
            self.reconnect()
            send(self.conn, data)
        except Exception:
            log.exception(traceback.format_exc())

    def disconnect(self):
        self.send_and_retry(Package('disconnect_client'))
        self.conn.detach()
        self.conn.close()

    def subscribe(self, topic: Text) -> None:
        self.send_and_retry(Package('subscribe', topic))

    def unsubscribe(self, topic: Text) -> None:
        self.send_and_retry(Package('unsubscribe', topic))

    def send(self, topic: Text, message: object) -> None:
        self.send_and_retry(Package('message', topic, message))

    def receive(self) -> Package:
        return recv(self.conn)


class Server:
    clint_counter = 0
    def __init__(self, host, port, verbose=True) -> None:
        self.host = host
        self.port = port
        self.verbose = verbose
        self.__run = False
        self.__topics: Dict[Text, Set[socket]] = dict()

    def init(self):
        self.__run = True
        self.process = Process(target=self.server_loop)
        self.process.start()

    def run_forever(self):
        while self.process.is_alive():
            sleep(0.1)

    def stop(self):
        self.__run = False

    def get_topics(self):
        client = Client(self.host, self.port)
        data = Package(type='get_topics')
        send(client.conn, data)
        response = client.receive()
        client.conn.close()
        assert response.type == 'topics'
        return response.message

    def __subscribe(self, data: Package, conn: socket):
        if data.topic not in self.__topics:
            self.__topics[data.topic] = {conn}
        else:
            self.__topics[data.topic].add(conn)

    def __unsubscribe(self, data: Package, conn: socket):
        self.__topics[data.topic].remove(conn)

    def __disconnect_client(self, data: Package, conn: socket):
        for topic, conns in self.__topics.items():
            if conn in conns:
                conns.remove(conn)

    def __message(self, data: Package, conn: socket):
        if data.topic in self.__topics:
            for _conn in self.__topics[data.topic]:
                send(_conn, data)

    def __get_topics(self, data: Package, conn: socket):
        topics = {
            topic: tuple([conn.getsockname() for conn in conns])
            for topic, conns in self.__topics.items()
        }
        response = Package(type='topics', message=topics)
        send(conn, response)

    def server_loop(self):
        log.debug('Processo inicializado.')
        
        with socket(AF_INET, SOCK_STREAM) as s:
            s.bind((self.host, self.port))
            s.listen()
            while self.__run:
                conn, addr = s.accept()
                thread = Thread(target=self.__client_handler, args=(conn, addr))
                thread.start()
                thread.name = f'pubsub-client{self.clint_counter}'
                self.clint_counter += 1

            s.detach()
            s.close()

    def __client_handler(self, conn, addr):
        funcs: Dict[Text, Callable] = {
            'message': self.__message,
            'subscribe': self.__subscribe,
            'unsubscribe': self.__unsubscribe,
            'get_topics': self.__get_topics,
            'disconnect_client': self.__disconnect_client
        }

        with conn:
            log.debug(f'Connected by {addr}')

            while True:
                try:
                    data: Package = recv(conn)

                except (
                    ConnectionResetError,
                    ConnectionAbortedError,
                    NonCompletedPackageReceivedError
                ):
                    for topic in self.__topics.keys():
                        if conn in self.__topics[topic]:
                            self.__topics[topic].remove(conn)
                    break
                except Exception:
                    log.exception(traceback.format_exc())
                    break
                try:
                    log.debug(data)
                    funcs[data.type](data, conn)

                except Exception:
                    log.exception(traceback.format_exc())
                    log.debug(self.__topics)
                    break

        log.debug(f'Desconnect by {addr}')


if __name__ == "__main__":
    HOST = '127.0.0.1'  # Standard loopback interface address (localhost)
    PORT = 65432        # Port to listen on (non-privileged ports are > 1023)
    server = Server(HOST, PORT)
    server.init()
    server.run_forever()
