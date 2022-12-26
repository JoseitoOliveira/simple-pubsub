from multiprocessing import Process
import lulapy
from lulapy import LulaPy_Client
from datetime import datetime


def foo(pubsub: LulaPy_Client, _id):
    pubsub.subscribe('foo')
    pubsub.send('started', '')
    data = pubsub.receive()
    print(f"{_id}:{datetime.now() - data['message']}")
    pubsub.close()


if __name__ == '__main__':
    pubsub = lulapy.begin()
    pubsub.subscribe('started')
    n = 100
    [Process(target=foo, args=(pubsub.new_client(), _id)).start()
     for _id in range(n)]

    for _ in range(n):
        pubsub.receive()

    pubsub.send('foo', datetime.now())
    pubsub.close()