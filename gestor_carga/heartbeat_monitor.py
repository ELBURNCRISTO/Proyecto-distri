#gestor_carga/heartbeat_monitor.py
import time
import json
import threading
import zmq
from comun.config import HEARTBEAT_TIMEOUT_SECONDS

class HeartbeatMonitor:
    """ Monitor heartbeat para el GC
    Se conecta al PUB del GA y detecta si esta vivo
    """
    def __init__(self, endpoint: str):
        self.endpoint = endpoint
        self.ga_vivo = False
        self.ultimo_timestamp = 0

    def start(self):
        ctx = zmq.Context()
        socket_sub = ctx.socket(zmq.SUB)
        socket_sub.connect(self.endpoint)
        socket_sub.setsockopt(zmq.SUBSCRIBE, b"HEARTBEAT")
        print(f"[GC] Monitor de heartbeat conectado a {self.endpoint}")

        def escuchar():
            while True:
                try:
                    topic, raw_msg = socket_sub.recv_multipart()
                    data = json.loads(raw_msg.decode("utf-8"))
                    self.ultimo_timestamp = data["timestamp"]
                    self.ga_vivo = True
                except Exception as e:
                    print(f"[GC] ERROR en monitor HB: {e}")

        def verificar():
            while True:
                time.sleep(1)
                ahora = time.time()
                if ahora - self.ultimo_timestamp > HEARTBEAT_TIMEOUT_SECONDS:
                    print("[GC] ALERTA: GA primario NO responde.")
                    self.ga_vivo = False
                else:
                    self.ga_vivo = True

        threading.Thread(target=escuchar, daemon=True).start()
        threading.Thread(target=verificar, daemon=True).start()
