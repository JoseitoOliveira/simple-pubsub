# Socket-PubSub

## A Python implementation publish-subscribe socket based

Basic usage:
```python
from socket_pubsub import Client, Server

if __name__ == '__main__':
    HOST = '127.0.0.1'
    PORT = 54321
    server = Server(HOST, PORT)
    server.init()

    client1 = Client(HOST, PORT)
    client1.subscribe('Topic')

    client2 = Client(HOST, PORT)
    client2.send('Topic', 'Message')

    print(client1.receive())
    
```
>_Package(type='message', topic='Topic', message='Message')_
