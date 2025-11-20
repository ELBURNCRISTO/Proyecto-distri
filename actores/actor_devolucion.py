#actores/actor_devolucion.py

import argparse
from comun.zeromq_utils import (
    create_context,
    create_sub_socket,
    create_req_socket,
    decode_message,
    encode_message,
)
from comun.config import (
    GC_SEDE1_ENDPOINT_PUB,
    GC_SEDE2_ENDPOINT_PUB,
    GA_SEDE1_ENDPOINT,
    GA_SEDE2_ENDPOINT,
    TOPIC_DEVOLUCION,
)


def get_endpoints_for_sede(sede: int):
    """Devuelve los endpoints del GC (PUB) y del GA segun sede"""
    if sede == 1:
        return GC_SEDE1_ENDPOINT_PUB, GA_SEDE1_ENDPOINT
    elif sede == 2:
        return GC_SEDE2_ENDPOINT_PUB, GA_SEDE2_ENDPOINT
    else:
        raise ValueError("La sede debe ser 1 o 2")


def run_actor_devolucion(sede: int):
    """Actor de Devolución:
    - Se suscribe al tópico DEVOLUCION publicado por el GC
    - Por cada mensaje, llama al GA con operacion = "devolucion"
    - No responde a los PS, solo procesa en segundo plano
    """
    endpoint_pub_gc, endpoint_ga = get_endpoints_for_sede(sede)

    print(f"[ActorDevolucion Sede {sede}] Suscrito a {endpoint_pub_gc} (topic DEVOLUCION)")
    print(f"[ActorDevolucion Sede {sede}] Comunicando con GA en {endpoint_ga}")

    context = create_context()

    # SUB al GC (escuchar devoluciones)
    socket_sub = create_sub_socket(context, endpoint_pub_gc, TOPIC_DEVOLUCION)
    # REQ al GA (aplicar devolución en la BD)
    socket_req_ga = create_req_socket(context, endpoint_ga)

    while True:
        topic, raw_msg = socket_sub.recv_multipart()
        evento = decode_message(raw_msg)

        operacion = evento.get("operacion")
        payload = evento.get("payload", {}) or {}

        if operacion != "devolucion":
            #Ignorar mensajes raros
            print(f"[ActorDevolucion Sede {sede}] Operación inesperada: {operacion}")
            continue
        print(f"[ActorPrestamos Sede {sede}] --- Nueva solicitud ---")
        print(f"[ActorDevolucion Sede {sede}] Procesando devolución: {payload}")
        solicitud_ga = {
            "operacion": "devolucion",
            "payload": payload,
        }

        try:
            socket_req_ga.send(encode_message(solicitud_ga))
            raw_resp = socket_req_ga.recv()
            resp_ga = decode_message(raw_resp)
            print(f"[ActorDevolucion Sede {sede}] Respuesta GA: {resp_ga}")
        except Exception as e:
            print(f"[ActorDevolucion Sede {sede}] ERROR al comunicarse con GA: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Actor de Devolución")
    parser.add_argument(
        "--sede",
        type=int,
        choices=[1, 2],
        required=True,
        help="Número de sede (1 o 2)",
    )
    args = parser.parse_args()
    run_actor_devolucion(args.sede)
