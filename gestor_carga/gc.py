#gestor_carga/gc.py

import argparse
from gestor_carga.heartbeat_monitor import HeartbeatMonitor
from comun.config import (
    GA_SEDE1_HEARTBEAT_ENDPOINT,
    GA_SEDE2_HEARTBEAT_ENDPOINT,
)
from comun.config import (
    GC_SEDE1_ENDPOINT_REQREP,
    GC_SEDE2_ENDPOINT_REQREP,
    GC_SEDE1_ENDPOINT_PUB,
    GC_SEDE2_ENDPOINT_PUB,
    ACTOR_PRESTAMOS_SEDE1_ENDPOINT,
    ACTOR_PRESTAMOS_SEDE2_ENDPOINT,
    TOPIC_DEVOLUCION,
    TOPIC_RENOVACION,
)
from comun.zeromq_utils import (
    create_context,
    create_rep_socket,
    create_req_socket,
    create_pub_socket,
    encode_message,
    decode_message,
)


def get_endpoints_for_sede(sede: int):
    """Devuelve los endpoints del GC (REQ/REP,PUB) y del actor de prestamos para una sede"""
    if sede == 1:
        return {
            "gc_reqrep": GC_SEDE1_ENDPOINT_REQREP,
            "gc_pub": GC_SEDE1_ENDPOINT_PUB,
            "actor_prestamos": ACTOR_PRESTAMOS_SEDE1_ENDPOINT,
        }
    elif sede == 2:
        return {
            "gc_reqrep": GC_SEDE2_ENDPOINT_REQREP,
            "gc_pub": GC_SEDE2_ENDPOINT_PUB,
            "actor_prestamos": ACTOR_PRESTAMOS_SEDE2_ENDPOINT,
        }
    else:
        raise ValueError("La sede debe ser 1 o 2")


def run_gc(sede: int):
    """Gestor de Carga (GC) de una sede
    Protocolo de mensaje desde PS → GC:
    {
        "operacion": "prestamo" | "devolucion" | "renovacion",
        "payload": {
            "libro_codigo": "L0001",
            "usuario_id": "U0001",
            "fecha_actual": "YYYY-MM-DD"
        }
    }
    """
    endpoints = get_endpoints_for_sede(sede)

    gc_reqrep_endpoint = endpoints["gc_reqrep"]
    gc_pub_endpoint = endpoints["gc_pub"]
    actor_prestamos_endpoint = endpoints["actor_prestamos"]

    #Monitor
    if sede == 1:
        hb_endpoint = GA_SEDE1_HEARTBEAT_ENDPOINT
    else:
        hb_endpoint = GA_SEDE2_HEARTBEAT_ENDPOINT

    monitor = HeartbeatMonitor(hb_endpoint)
    monitor.start()

    print(f"[GC Sede {sede}] Monitor de heartbeat iniciado en {hb_endpoint}")
    # ---------------------------------------


    print(f"[GC Sede {sede}] Escuchando PS en {gc_reqrep_endpoint}")
    print(f"[GC Sede {sede}] Publicando eventos en {gc_pub_endpoint}")
    print(f"[GC Sede {sede}] Actor de préstamos en {actor_prestamos_endpoint}")

    context = create_context()
    #Socket (REP) para hablar con PS
    socket_rep_ps = create_rep_socket(context, gc_reqrep_endpoint)
    #Socket (PUB) para enviar devoluciones/renovaciones a los actores
    socket_pub = create_pub_socket(context, gc_pub_endpoint)
    # Socket (REQ) para hablar con el Actor de prestamos
    socket_req_actor = create_req_socket(context, actor_prestamos_endpoint)

    while True:
        #Recibir mensaje desde PS
        raw = socket_rep_ps.recv()
        msg_ps = decode_message(raw)

        operacion = msg_ps.get("operacion")
        payload = msg_ps.get("payload", {}) or {}

        if operacion == "prestamo":
            usar_backup = False
            try:
                if not monitor.ga_vivo:
                    usar_backup = True
                    print("[GC] Usando GA de respaldo para esta solicitud prestamo.")
            except NameError:
                #Por si no existe monitor asumimos primario
                usar_backup = False
            #Prestamo -> llamada síncrona al actor de prestamos
            msg_actor = {
                "operacion": "prestamo",
                "payload": payload,
                "usar_backup": usar_backup,
            }
            try:
                socket_req_actor.send(encode_message(msg_actor))
                raw_resp = socket_req_actor.recv()
                resp_actor = decode_message(raw_resp)
            except Exception as e:
                resp_actor = {
                    "ok": False,
                    "razon": "ERROR_ACTOR_PRESTAMOS",
                    "mensaje": f"Error al comunicarse con el Actor de Prestamos: {e}",
                }

            #Responder al PS con el resultado real
            socket_rep_ps.send(encode_message(resp_actor))

        elif operacion == "devolucion":
            #Devolución -> responder al PS
            ack = {
                "ok": True,
                "tipo": "devolucion",
                "mensaje": "Solicitud de devolución recibida y encolada para procesamiento.",
            }
            socket_rep_ps.send(encode_message(ack))

            #Publicar evento para actores
            evento = {
                "operacion": "devolucion",
                "payload": payload,
                "sede": sede,
            }
            socket_pub.send_multipart(
                [TOPIC_DEVOLUCION, encode_message(evento)]
            )

        elif operacion == "renovacion":
            #Renovación -> responder al PS
            ack = {
                "ok": True,
                "tipo": "renovacion",
                "mensaje": "Solicitud de renovacion recibida y encolada para procesamiento.",
            }
            socket_rep_ps.send(encode_message(ack))

            #Publicar evento para actores
            evento = {
                "operacion": "renovacion",
                "payload": payload,
                "sede": sede,
            }
            socket_pub.send_multipart(
                [TOPIC_RENOVACION, encode_message(evento)]
            )

        else:
            #Operacion desconocida
            resp = {
                "ok": False,
                "razon": "OPERACION_DESCONOCIDA",
                "mensaje": f"Operación '{operacion}' no soportada por el GC.",
            }
            socket_rep_ps.send(encode_message(resp))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gestor de Carga (GC)")
    parser.add_argument(
        "--sede",
        type=int,
        choices=[1, 2],
        required=True,
        help="Número de sede (1 o 2)",
    )
    args = parser.parse_args()
    run_gc(args.sede)
