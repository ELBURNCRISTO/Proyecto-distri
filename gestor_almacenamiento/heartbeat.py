#gestor_almacenamiento/heartbeat.py
import time
import json
import threading
import zmq
def start_ga_heartbeat(endpoint: str, sede: int, interval: float = 2.0):
    """Inicia un hilo que publica heartbeats desde el GA
    El GC usa estos heartbeats para detectar fallas
    """
    ctx = zmq.Context()
    socket_pub = ctx.socket(zmq.PUB)
    socket_pub.bind(endpoint)

    def run():
        while True:
            msg = {
                "sede": sede,
                "estado": "OK",
                "timestamp": time.time(),
            }
            socket_pub.send_multipart([
                b"HEARTBEAT",
                json.dumps(msg).encode()
            ])
            time.sleep(interval)

    hilo = threading.Thread(target=run, daemon=True)
    hilo.start()
