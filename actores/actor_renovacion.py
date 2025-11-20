# actores/actor_renovacion.py

import argparse
import time  # <--- Importante
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
    TOPIC_RENOVACION,
)


def get_endpoints_for_sede(sede: int):
    """Devuelve endpoints del GC (PUB) y del GA segun sede"""
    if sede == 1:
        return GC_SEDE1_ENDPOINT_PUB, GA_SEDE1_ENDPOINT
    elif sede == 2:
        return GC_SEDE2_ENDPOINT_PUB, GA_SEDE2_ENDPOINT
    else:
        raise ValueError("La sede debe ser 1 o 2")


def run_actor_renovacion(sede: int):
    """Actor de Renovacion:
    - Se suscribe al topico RENOVACION publicado por el GC
    - Por cada mensaje llama al GA con operacion = renovacion
    - Implementa espera activa si el GA cae.
    """
    endpoint_pub_gc, endpoint_ga = get_endpoints_for_sede(sede)

    print(f"[ActorRenovacion Sede {sede}] Suscrito a {endpoint_pub_gc} (topic RENOVACION)")
    print(f"[ActorRenovacion Sede {sede}] Comunicando con GA en {endpoint_ga}")

    context = create_context()

    # SUB al GC
    socket_sub = create_sub_socket(context, endpoint_pub_gc, TOPIC_RENOVACION)
    # REQ al GA
    socket_req_ga = create_req_socket(context, endpoint_ga)

    while True:
        topic, raw_msg = socket_sub.recv_multipart()
        evento = decode_message(raw_msg)

        operacion = evento.get("operacion")
        payload = evento.get("payload", {}) or {}

        if operacion != "renovacion":
            print(f"[ActorRenovacion Sede {sede}] Operación inesperada: {operacion}")
            continue

        print(f"[ActorRenovacion Sede {sede}] --- Nueva solicitud ---")
        print(f"[ActorRenovacion Sede {sede}] Procesando renovación: {payload}")

        solicitud_ga = {
            "operacion": "renovacion",
            "payload": payload,
        }

        # --- LOGICA DE ESPERA Y REINTENTO (LAZY PIRATE) ---
        while True:
            try:
                socket_req_ga.send(encode_message(solicitud_ga))
                raw_resp = socket_req_ga.recv()
                resp_ga = decode_message(raw_resp)
                print(f"[ActorRenovacion Sede {sede}] Respuesta GA: {resp_ga}")
                break  # Éxito

            except Exception as e:
                print(f"[ActorRenovacion Sede {sede}] GA no responde o está caído ({e}).")
                print(f"[ActorRenovacion Sede {sede}] Esperando a que reviva para reintentar...")

                socket_req_ga.close()
                time.sleep(2)
                socket_req_ga = create_req_socket(context, endpoint_ga)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Actor de Renovación")
    parser.add_argument(
        "--sede",
        type=int,
        choices=[1, 2],
        required=True,
        help="Número de sede (1 o 2)",
    )
    args = parser.parse_args()
    run_actor_renovacion(args.sede)