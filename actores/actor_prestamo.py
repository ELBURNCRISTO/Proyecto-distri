# actores/actor_prestamo.py

import argparse
from comun.zeromq_utils import (
    create_context,
    create_rep_socket,
    create_req_socket,
    encode_message,
    decode_message,
)
from comun.config import (
    ACTOR_PRESTAMOS_SEDE1_ENDPOINT,
    ACTOR_PRESTAMOS_SEDE2_ENDPOINT,
    GA_SEDE1_ENDPOINT,
    GA_SEDE2_ENDPOINT,
)


def get_endpoints_for_sede(sede: int):
    """Devuelve: endpoint donde escucha al GC,
    GA primario y GA de respaldo
    """
    if sede == 1:
        return (
            ACTOR_PRESTAMOS_SEDE1_ENDPOINT,
            GA_SEDE1_ENDPOINT,  #primario
            GA_SEDE2_ENDPOINT,  #respaldo
        )
    elif sede == 2:
        return (
            ACTOR_PRESTAMOS_SEDE2_ENDPOINT,
            GA_SEDE2_ENDPOINT,  #primario
            GA_SEDE1_ENDPOINT,  #respaldo
        )
    else:
        raise ValueError("La sede debe ser 1 o 2")


def run_actor_prestamos(sede: int):
    endpoint_actor, endpoint_ga_primario, endpoint_ga_backup = get_endpoints_for_sede(sede)

    print(f"[ActorPrestamos Sede {sede}] Esperando solicitudes en {endpoint_actor}")
    print(f"[ActorPrestamos Sede {sede}] GA primario:  {endpoint_ga_primario}")
    print(f"[ActorPrestamos Sede {sede}] GA respaldo:  {endpoint_ga_backup}")

    context = create_context()

    socket_rep_gc = create_rep_socket(context, endpoint_actor)
    socket_req_ga_primario = create_req_socket(context, endpoint_ga_primario)
    socket_req_ga_backup = create_req_socket(context, endpoint_ga_backup)

    while True:
        raw = socket_rep_gc.recv()
        msg_gc = decode_message(raw)

        operacion = msg_gc.get("operacion")
        payload = msg_gc.get("payload", {}) or {}
        usar_backup = msg_gc.get("usar_backup", False)

        print(f"[ActorPrestamos Sede {sede}] --- Nueva solicitud ---")
        print(f"[ActorPrestamos Sede {sede}] Operación: {operacion}")
        print(f"[ActorPrestamos Sede {sede}] Payload:    {payload}")

        if operacion != "prestamo":
            resp = {
                "ok": False,
                "razon": "OPERACION_INVALIDA",
                "mensaje": "Este actor solo maneja 'prestamo'.",
            }
            socket_rep_gc.send(encode_message(resp))
            continue

        solicitud_ga = {"operacion": "prestamo", "payload": payload}
        resp_ga = None

        try:
            if usar_backup:
                print(f"[ActorPrestamos Sede {sede}] Usando GA de respaldo (heartbeat).")
                socket_req_ga_backup.send(encode_message(solicitud_ga))
                raw_resp = socket_req_ga_backup.recv()
                resp_ga = decode_message(raw_resp)
            else:
                print(f"[ActorPrestamos Sede {sede}] Usando GA primario.")
                socket_req_ga_primario.send(encode_message(solicitud_ga))
                raw_resp = socket_req_ga_primario.recv()
                resp_ga = decode_message(raw_resp)

        except Exception as e:
            print(f"[ActorPrestamos Sede {sede}] ERROR GA primario: {e}")
            print(f"[ActorPrestamos Sede {sede}] Intentando GA de respaldo...")

            try:
                socket_req_ga_backup.send(encode_message(solicitud_ga))
                raw_resp = socket_req_ga_backup.recv()
                resp_ga = decode_message(raw_resp)
            except Exception as e_backup:
                resp_ga = {
                    "ok": False,
                    "razon": "GA_INACCESIBLE",
                    "mensaje": f"Error en ambos GA: {e_backup}",
                }

        print(f"[ActorPrestamos Sede {sede}] Respuesta GA: {resp_ga}")

        socket_rep_gc.send(encode_message(resp_ga))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Actor de Prestamos")
    parser.add_argument("--sede", type=int, choices=[1, 2], required=True)
    args = parser.parse_args()
    run_actor_prestamos(args.sede)
