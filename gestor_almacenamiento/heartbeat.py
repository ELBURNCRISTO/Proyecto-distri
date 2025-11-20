# gestor_almacenamiento/heartbeat.py
import time
import json
import threading
import zmq


def start_ga_heartbeat(endpoint: str, sede: int, data_callback, interval: float = 1.0):
    """
    Inicia un hilo que publica heartbeats.
    data_callback: función que debe devolver un dict con {'version': int, 'estado': str}
    """
    ctx = zmq.Context()
    socket_pub = ctx.socket(zmq.PUB)
    socket_pub.bind(endpoint)

    def run():
        while True:
            # Obtenemos datos frescos del GA (versión actual de la BD)
            extra_data = data_callback()  # Ej: {'version': 5, 'estado': 'OK'}

            msg = {
                "sede": sede,
                "timestamp": time.time(),
            }
            msg.update(extra_data)  # Mezclar con version y estado

            try:
                socket_pub.send_multipart([
                    b"HEARTBEAT",
                    json.dumps(msg).encode()
                ])
            except Exception as e:
                print(f"[Heartbeat] Error enviando: {e}")

            time.sleep(interval)

    hilo = threading.Thread(target=run, daemon=True)
    hilo.start()