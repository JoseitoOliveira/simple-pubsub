from multiprocessing import Process
from socket import AF_INET, SOCK_STREAM, socket
from threading import Thread
from typing import Callable, Dict, List, Set, Text

from socket_tools import recv, send

HOST = '127.0.0.1'  # Standard loopback interface address (localhost)
PORT = 65432        # Port to listen on (non-privileged ports are > 1023)


topics: Dict[Text, Set[socket]] = dict()


def subscribe(data, conn: socket):
    if data['topic'] not in topics:
        topics[data['topic']] = {conn}
    else:
        topics[data['topic']].add(conn)


def unsubscribe(data, conn: socket):
    topics[data['topic']].remove(conn)


def message(data, conn: socket):
    if data['topic'] in topics:
        for _conn in topics[data['topic']]:
            send(_conn, data)


def begin():
    broker = Process(target=lulapy_broker)
    broker.start()
    while not broker.is_alive():
        pass


def new_client(conn, addr):
    funcs: Dict[Text, Callable] = {
        'message': message,
        'subscribe': subscribe,
        'unsubscribe': unsubscribe,
    }

    with conn:
        print('Connected by', addr)
        while True:
            try:
                data = recv(conn)
            except ConnectionResetError:
                for topic in topics.keys():
                    if conn in topics[topic]:
                        topics[topic].remove(conn)
                break
            print(data)
            _type = data['type']

            funcs[_type](data, conn)

    print('Desconnect by', addr)


def lulapy_broker():
    with socket(AF_INET, SOCK_STREAM) as s:
        s.bind((HOST, PORT))
        s.listen()
        while True:
            conn, addr = s.accept()
            Thread(target=new_client, args=(conn, addr)).start()


if __name__ == "__main__":
    begin()
