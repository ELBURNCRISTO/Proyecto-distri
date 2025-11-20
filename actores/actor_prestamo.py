# actores/actor_prestamo.py

import argparse
import time
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
    if sede == 1:
        return (
            ACTOR_PRESTAMOS_SEDE1_ENDPOINT,
            GA_SEDE1_ENDPOINT,  # primario
            GA_SEDE2_ENDPOINT,  # respaldo
        )
    elif sede == 2:
        return (
            ACTOR_PRESTAMOS_SEDE2_ENDPOINT,
            GA_SEDE2_ENDPOINT,  # primario
            GA_SEDE1_ENDPOINT,  # respaldo
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
        try:
            raw = socket_rep_gc.recv()
            msg_gc = decode_message(raw)
        except Exception as e:
            print(f"[Actor] Error al recibir del GC: {e}")
            # Reiniciar socket REP si es necesario (raro en REP)
            continue

        operacion = msg_gc.get("operacion")
        payload = msg_gc.get("payload", {}) or {}
        usar_backup_flag = msg_gc.get("usar_backup", False)

        print(f"[ActorPrestamos Sede {sede}] --- Solicitud: {operacion} | {payload.get('libro_codigo')} ---")

        if operacion != "prestamo":
            resp = {"ok": False, "razon": "INVALID", "mensaje": "Solo prestamos"}
            socket_rep_gc.send(encode_message(resp))
            continue

        solicitud_ga = {"operacion": "prestamo", "payload": payload}
        resp_ga = None

        # --- LOGICA DE FAILOVER ROBUSTA ---
        exito = False

        # 1. INTENTO CON PRIMARIO (si GC no forzó backup)
        if not usar_backup_flag:
            try:
                print(f"[Actor] Contactando GA Primario...")
                socket_req_ga_primario.send(encode_message(solicitud_ga))
                raw_resp = socket_req_ga_primario.recv()
                resp_ga = decode_message(raw_resp)
                exito = True
            except Exception as e:
                print(f"[Actor] Fallo GA Primario ({e}). Cerrando socket y probando respaldo.")
                socket_req_ga_primario.close()
                socket_req_ga_primario = create_req_socket(context, endpoint_ga_primario)
                usar_backup_flag = True  # Forzar paso al backup

        # 2. INTENTO CON RESPALDO (si falló primario o GC lo pidió)
        if not exito and usar_backup_flag:
            try:
                print(f"[Actor] Contactando GA Respaldo...")
                socket_req_ga_backup.send(encode_message(solicitud_ga))
                raw_resp = socket_req_ga_backup.recv()
                resp_ga = decode_message(raw_resp)
                exito = True
            except Exception as e:
                print(f"[Actor] Fallo GA Respaldo ({e}). Cerrando socket.")
                socket_req_ga_backup.close()
                socket_req_ga_backup = create_req_socket(context, endpoint_ga_backup)

                resp_ga = {
                    "ok": False,
                    "razon": "GA_CRASH",
                    "mensaje": "Ambos Gestores de Almacenamiento están inaccesibles.",
                }

        print(f"[ActorPrestamos Sede {sede}] Resultado: {resp_ga.get('ok')}")
        socket_rep_gc.send(encode_message(resp_ga))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Actor de Prestamos")
    parser.add_argument("--sede", type=int, choices=[1, 2], required=True)
    args = parser.parse_args()
    run_actor_prestamos(args.sede)