import struct
import pickle


def send_bytes(sock, bytes):
    # Prefix each message with a 4-byte length (network byte order)
    data = struct.pack('>I', len(bytes)) + bytes
    sock.sendall(data)


def recv_bytes(sock):
    # Read message length and unpack it into an integer
    raw_msglen = __recvall(sock, 4)
    if not raw_msglen:
        raise Exception('Unexpected message lenght.')
    msglen = struct.unpack('>I', raw_msglen)[0]
    # Read the message data
    return __recvall(sock, msglen)


def send(sock, obj):
    data = pickle.dumps(obj)
    send_bytes(sock, data)


def recv(sock):
    bytes = recv_bytes(sock)
    if bytes:
        obj = pickle.loads(bytes)
        return obj


def __recvall(sock, n):
    # Helper function to recv n bytes or return None if EOF is hit
    data = bytearray()
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            return None
        data.extend(packet)
    return data
