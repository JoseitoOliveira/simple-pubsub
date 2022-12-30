import logging
from random import randint

import socket_pubsub
from socket_pubsub import Client, Package, Server

HOST = '127.0.0.1'  # Standard loopback interface address (localhost)
for handlers in socket_pubsub.log.handlers:
    handlers.setLevel(logging.DEBUG)

def test_init_server():
    server = Server(HOST, randint(1024, 65432))
    server.init()
    assert server.process.is_alive()
    server.stop()

def test_topic_subscribe():
    TOPIC = 'topic A'
    port = randint(1024, 65432)
    server = Server(HOST, port)
    server.init()

    client1 = Client(HOST, port)
    client1.subscribe(TOPIC)
    
    topics = server.get_topics()
    assert topics == {TOPIC: ((HOST, port),)}
    
    server.stop()

def test_topic_unsubscribe():
    TOPIC = 'topic A'
    port = randint(1024, 65432)
    server = Server(HOST, port)
    server.init()

    client1 = Client(HOST, port)
    client1.subscribe(TOPIC)
    client1.unsubscribe(TOPIC)
    
    topics = server.get_topics()
    assert topics == {}
    
    server.stop()

def test_send_receice():
    MSG = 'Hello, world!'
    TOPIC = 'topic A'
    port = randint(1024, 65432)
    server = Server(HOST, port)
    server.init()

    client1 = Client(HOST, port)
    client1.subscribe(TOPIC)

    client2 = Client(HOST, port)
    client2.send(TOPIC, MSG)

    assert client1.receive() == Package('message', TOPIC, MSG)

    server.stop()

def test_send_to_void_topic():
    MSG = 'Hello, world!'
    TOPIC = 'topic A'
    port = randint(1024, 65432)
    server = Server(HOST, port)
    server.init()

    client1 = Client(HOST, port)
    client1.send(TOPIC, MSG)

    server.stop()

def test_send_a_object():
    TOPIC = 'topic A'
    port = randint(1024, 65432)
    server = Server(HOST, port)
    server.init()

    client1 = Client(HOST, port)
    client1.subscribe(TOPIC)

    client1.send(TOPIC, [1,2,3,4,5])

    msg = client1.receive()
    assert msg.message == [1,2,3,4,5]


def test_topic_multiple_unsubscribe():
    TOPIC = 'topic A'
    port = randint(1024, 65432)
    server = Server(HOST, port)
    server.init()

    client1 = Client(HOST, port)
    client1.subscribe(TOPIC)
    try:
        client1.unsubscribe(TOPIC)
        client1.unsubscribe(TOPIC)
        client1.unsubscribe(TOPIC)
        client1.subscribe(TOPIC)
    except Exception:
        assert False


