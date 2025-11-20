# ps/ps.py
import argparse
import time
from pathlib import Path
from comun.zeromq_utils import (
    create_context,
    create_req_socket,
    encode_message,
    safe_recv,
)
from comun.config import (
    GC_SEDE1_ENDPOINT_REQREP,
    GC_SEDE2_ENDPOINT_REQREP,
)


def get_gc_endpoint_for_sede(sede: int) -> str:
    """Devuelve el endpoint del Gestor de Carga (GC) a la sede dada"""
    if sede == 1:
        return GC_SEDE1_ENDPOINT_REQREP
    elif sede == 2:
        return GC_SEDE2_ENDPOINT_REQREP
    else:
        raise ValueError("La sede debe ser 1 o 2")


def parse_line(line: str):
    """Parsea una línea del archivo de solicitudes"""
    line = line.strip()
    if not line or line.startswith("#"):
        return None  # línea vacía o comentario

    partes = line.split(";")
    if len(partes) != 4:
        print(f"[PS] Línea inválida (se esperan 4 campos): {line}")
        return None
    operacion_txt, cod_libro, usuario_id, fecha = [p.strip() for p in partes]
    operacion_txt = operacion_txt.upper()
    if operacion_txt not in ("PRESTAMO", "DEVOLUCION", "RENOVACION"):
        print(f"[PS] Operacion desconocida: {operacion_txt} en línea: {line}")
        return None

    # Mapeo
    if operacion_txt == "PRESTAMO":
        operacion = "prestamo"
    elif operacion_txt == "DEVOLUCION":
        operacion = "devolucion"
    else:
        operacion = "renovacion"
    payload = {
        "libro_codigo": cod_libro,
        "usuario_id": usuario_id,
        "fecha_actual": fecha,
    }
    return operacion, payload


def run_ps(sede: int, archivo_solicitudes: Path):
    """PS:
    - Lee un archivo de texto línea por línea
    - Por cada línea válida envía una solicitud al GC
    - Implementa 'Lazy Pirate': si hay timeout, reinicia el socket para no estallar.
    """
    if not archivo_solicitudes.exists():
        print(f"[PS] El archivo de solicitudes no existe: {archivo_solicitudes}")
        return
    gc_endpoint = get_gc_endpoint_for_sede(sede)
    print(f"[PS Sede {sede}] Usando GC en {gc_endpoint}")
    print(f"[PS Sede {sede}] Leyendo solicitudes de {archivo_solicitudes}")

    context = create_context()
    socket_req_gc = create_req_socket(context, gc_endpoint)

    with archivo_solicitudes.open("r", encoding="utf-8") as f:
        for linea in f:
            parsed = parse_line(linea)
            if parsed is None:
                continue  # vacía o inválida

            operacion, payload = parsed
            msg = {
                "operacion": operacion,
                "payload": payload,
            }
            print(f"[PS] Enviando: {msg}")

            try:
                # Intentar enviar
                socket_req_gc.send(encode_message(msg))

                # Esperar respuesta
                resp = safe_recv(socket_req_gc)

                if resp is None:
                    print("[PS] No se recibió respuesta del GC (timeout). Reiniciando conexión...")
                    # FIX: Cerrar socket dañado y crear uno nuevo para la siguiente iteración
                    socket_req_gc.close()
                    socket_req_gc = create_req_socket(context, gc_endpoint)
                else:
                    print(f"[PS] Respuesta GC: {resp}")

            except Exception as e:
                print(f"[PS] Error crítico de socket: {e}")
                print("[PS] Reiniciando conexión...")
                socket_req_gc.close()
                socket_req_gc = create_req_socket(context, gc_endpoint)

            # Pausa entre solicitudes
            time.sleep(0.5)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Proceso Solicitante (PS)")
    parser.add_argument(
        "--sede",
        type=int,
        choices=[1, 2],
        required=True,
        help="Numero de sede a la que se envian las solicitudes",
    )
    parser.add_argument(
        "--archivo",
        type=str,
        required=True,
        help="Ruta al archivo de solicitudes",
    )
    args = parser.parse_args()

    run_ps(
        sede=args.sede,
        archivo_solicitudes=Path(args.archivo),
    )