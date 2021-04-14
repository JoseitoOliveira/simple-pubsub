from socket import AF_INET, SOCK_STREAM, socket
from typing import Any, Text

from socket_tools import recv, send

HOST = '127.0.0.1'  # The server's hostname or IP address
PORT = 65432        # The port used by the server


class LulaPy_Client:
    def __init__(self, HOST, PORT) -> None:
        self.conn = socket(AF_INET, SOCK_STREAM)
        self.addr_port = (HOST, PORT)
        self.conn.connect(self.addr_port)

    def reconnect(self):
        self.conn.connect(self.addr_port)

    def subscribe(self, topic: Text) -> None:
        try:
            send(self.conn, {'type': 'subscribe', 'topic': topic})
        except ConnectionResetError:
            self.reconnect()
        finally:
            send(self.conn, {'type': 'subscribe', 'topic': topic})

    def unsubscribe(self, topic: Text) -> None:
        try:
            send(self.conn, {'type': 'unsubscribe', 'topic': topic})
        except ConnectionResetError:
            self.reconnect()
        finally:
            send(self.conn, {'type': 'unsubscribe', 'topic': topic})

    def send(self, topic: Text, message: Any) -> None:
        try:
            send(self.conn,
                 {'type': 'message', 'topic': topic, 'message': message})
        except ConnectionResetError:
            self.reconnect()
        finally:
            send(self.conn,
                 {'type': 'message', 'topic': topic, 'message': message})

    def receive(self):
        return recv(self.conn)
