#comun/zeromq_utils.py
import json
import zmq
import time

#SERIALIZACIÓN Y DESERIALIZACION
def encode_message(data: dict) -> bytes:
    """Dict Python a bytes JSON para enviar por ZeroMQ"""
    return json.dumps(data).encode("utf-8")

def decode_message(raw: bytes) -> dict:
    """Convierte bytes recibidos por ZeroMQ en Dict Python"""
    return json.loads(raw.decode("utf-8"))

#CREACION DE SOCKETS
def create_context():
    """Crea un contexto global de ZeroMQ (se llama una vez por proceso)"""
    return zmq.Context()

def create_req_socket(context: zmq.Context, endpoint: str, timeout_ms=3000):
    """Crea un socket REQ con timeout"""
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.RCVTIMEO, timeout_ms)
    socket.setsockopt(zmq.SNDTIMEO, timeout_ms)
    socket.connect(endpoint)
    return socket

def create_rep_socket(context: zmq.Context, endpoint: str):
    """Crea un socket REP para recibir solicitudes"""
    socket = context.socket(zmq.REP)
    socket.bind(endpoint)
    return socket


def create_pub_socket(context: zmq.Context, endpoint: str):
    """Crea un socket PUB, los actores se conectarán con SUB"""
    socket = context.socket(zmq.PUB)
    socket.bind(endpoint)
    return socket


def create_sub_socket(context: zmq.Context, endpoint: str, topic: bytes):
    """Crea un socket SUB suscrito a un tópico específico"""
    socket = context.socket(zmq.SUB)
    socket.connect(endpoint)
    socket.setsockopt(zmq.SUBSCRIBE, topic)
    return socket

#ENVÍO/RECEPCIÓN DE MENSAJES
def safe_send(socket, data: dict):
    """Envia un mensaje en formato dict → JSON → bytes"""
    try:
        socket.send(encode_message(data))
        return True
    except zmq.ZMQError:
        return False

def safe_recv(socket):
    """Recibe un mensaje JSON y lo convierte a dict, timeout -> retorna None"""
    try:
        raw = socket.recv()
        return decode_message(raw)
    except zmq.ZMQError:
        return None

#HEARTBEAT DETECCIÓN DE FALLOS
def start_heartbeat_sender(context, endpoint: str, interval=2.0):
    """Crea un socket PUSH para enviar heartbeats, devuelve la función que se debe llamar dentro de un loop"""
    socket = context.socket(zmq.PUSH)
    socket.connect(endpoint)

    def beat():
        try:
            socket.send(b"HB")
        except:
            pass

    return beat


def start_heartbeat_listener(context, endpoint: str):
    """Recibe heartbeat, necesario para detectar si un GA esta vivo"""
    socket = context.socket(zmq.PULL)
    socket.bind(endpoint)

    return socket

#UTILIDAD
def wait_with_timeout(seconds: float):
    """Pausa el proceso con sleep sin bloquear totalmente"""
    end = time.time() + seconds
    while time.time() < end:
        time.sleep(0.05)
