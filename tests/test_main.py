import logging
from random import randint

import simple_pubsub
from simple_pubsub import Client, Package, Server

HOST = '127.0.0.1'  # Standard loopback interface address (localhost)
for handlers in simple_pubsub.log.handlers:
    handlers.setLevel(logging.DEBUG)

def test_init_server():
    server = Server(HOST, randint(1024, 65432))
    server.init()
    assert server.process.is_alive()
    server.stop()

def test_topic_subscribe():
    port = randint(1024, 65432)
    server = Server(HOST, port)
    server.init()

    client1 = Client(HOST, port)
    client1.subscribe('topic A')
    
    topics = server.get_topics()
    assert topics == {'topic A': ((HOST, port),)}
    
    server.stop()


# def test_new_client():
#     server = Server(HOST, PORT)
#     server.init()


#     pubsubA = Client(HOST, PORT)
#     pubsubB = Client(HOST, PORT)
#     pubsubA.subscribe('A')
#     pubsubB.subscribe('B')

#     pubsubA.send('B', 'Olá, B')

#     receivedB = pubsubB.receive()
#     assert receivedB == Package('message', 'B', 'Olá, B')

#     pubsubB.send('A', 'Olá, A')

#     receivedA = pubsubA.receive()
#     assert receivedA == Package('message', 'A', 'Olá, A')


# def echo(pb: lulapy.LulaPy_Client):
#     pb.subscribe('foo')
#     data = pb.receive()
#     print(data)
#     pb.send(topic='echo', message=data['message'])


# def test_comunication_with_process():

#     pubsub = lulapy.begin()
#     pubsub.subscribe('echo')

#     Process(target=echo, args=(pubsub.new_client(),)).start()
#     sleep(0.5)
#     pubsub.send('foo', 'bar')
#     data = pubsub.receive()

#     assert data == {'message': 'bar',
#                     'topic': 'echo', 'type': 'message'}

#     pubsub.close()


# def test_create_pubsub_server():
#     pubsubA = lulapy.begin()
#     pubsubA.subscribe('A')
#     pubsubA.send('A', 'Olá mundo')

#     received = pubsubA.receive()

#     assert received == {'message': 'Olá mundo',
#                         'topic': 'A', 'type': 'message'}

if __name__ == '__main__':
    test_topic_subscribe()