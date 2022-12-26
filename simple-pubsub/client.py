from socket import AF_INET, SOCK_STREAM, socket
from typing import Any, Text

from socket_tools import recv, send


class Client:
    def __init__(self, host, port) -> None:
        self.conn = socket(AF_INET, SOCK_STREAM)
        self.host = host
        self.port = port
        self.conn.connect((self.host, self.port))

    def reconnect(self):
        self.conn.connect((self.host, self.port))

    def subscribe(self, topic: Text) -> None:
        data = {'type': 'subscribe', 'topic': topic}
        try:
            return send(self.conn, data)
        except ConnectionResetError:
            self.reconnect()
            send(self.conn, data)

    def unsubscribe(self, topic: Text) -> None:
        data = {'type': 'unsubscribe', 'topic': topic}
        try:
            return send(self.conn, data)
        except ConnectionResetError:
            self.reconnect()
            send(self.conn, data)

    def send(self, topic: Text, message: Any) -> None:
        data = {'type': 'message', 'topic': topic, 'message': message}
        try:
            return send(self.conn, data)
        except ConnectionResetError:
            self.reconnect()
            send(self.conn, data)

    def receive(self):
        return recv(self.conn)
