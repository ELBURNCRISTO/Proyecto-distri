# gestor_almacenamiento/ga.py
import argparse
import json
import random
import threading
import time
import zmq
import shutil  # Para copiar archivos
from datetime import datetime, timedelta
from pathlib import Path
from comun.config import (
    get_bd_paths_for_sede,
    GA_SEDE1_ENDPOINT,
    GA_SEDE2_ENDPOINT,
    GA_SEDE1_HEARTBEAT_ENDPOINT,
    GA_SEDE2_HEARTBEAT_ENDPOINT,
    HEARTBEAT_INTERVAL_SECONDS,
)
from gestor_almacenamiento.heartbeat import start_ga_heartbeat
from comun.zeromq_utils import (
    create_context,
    create_rep_socket,
    create_sub_socket,  # Necesario para escuchar al otro GA
    encode_message,
    decode_message,
)

# Variable global para la BD en memoria y Lock para hilos
db_lock = threading.Lock()
db_in_memory = {}


# UTILIDADES BD
def load_db(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
        # Asegurar que existe el campo version
        if "version" not in data:
            data["version"] = 0
        return data


def save_db(path: Path, db: dict):
    path.parent.mkdir(exist_ok=True, parents=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(db, f, ensure_ascii=False, indent=2)


def ensure_initial_data(primaria_path: Path):
    if primaria_path.exists():
        return
    print(f"[GA] Generando BD inicial en {primaria_path} ...")
    libros = []
    for i in range(1, 1001):
        libros.append({
            "codigo": f"L{i:04d}",
            "titulo": f"Libro {i}",
            "autor": f"Autor {((i - 1) % 50) + 1}",
            "ejemplares_totales": random.randint(1, 4),
            "ejemplares_disponibles": random.randint(1, 4),
            "prestamos": []
        })
    # Iniciar versión en 0
    db = {"version": 0, "libros": libros}
    save_db(primaria_path, db)


# OPERACIONES (Modifican la versión)
def incrementar_version(db: dict):
    db["version"] = db.get("version", 0) + 1
    print(f"[GA] BD actualizada a versión {db['version']}")


def find_libro(db: dict, codigo: str):
    for libro in db.get("libros", []):
        if libro["codigo"] == codigo:
            return libro
    return None


def op_prestamo(db: dict, payload: dict):
    codigo = payload.get("libro_codigo")
    usuario_id = payload.get("usuario_id")
    fecha_actual = payload.get("fecha_actual")

    libro = find_libro(db, codigo)
    if not libro:
        return db, {"ok": False, "mensaje": "Libro no existe"}
    if libro["ejemplares_disponibles"] <= 0:
        return db, {"ok": False, "mensaje": "Sin ejemplares"}

    # Lógica simple préstamo
    libro["ejemplares_disponibles"] -= 1
    libro["prestamos"].append({
        "usuario_id": usuario_id,
        "fecha_entrega": str(datetime.fromisoformat(fecha_actual).date() + timedelta(days=14)),
        "renovaciones": 0
    })

    incrementar_version(db)  # IMPORTANTE
    return db, {"ok": True, "mensaje": "Prestamo exitoso", "version_bd": db["version"]}


def op_devolucion(db: dict, payload: dict):
    codigo = payload.get("libro_codigo")
    usuario_id = payload.get("usuario_id")
    libro = find_libro(db, codigo)
    if not libro: return db, {"ok": False, "mensaje": "No existe"}

    for i, p in enumerate(libro["prestamos"]):
        if p["usuario_id"] == usuario_id:
            libro["prestamos"].pop(i)
            libro["ejemplares_disponibles"] += 1
            incrementar_version(db)  # IMPORTANTE
            return db, {"ok": True, "mensaje": "Devolucion exitosa", "version_bd": db["version"]}

    return db, {"ok": False, "mensaje": "No tiene prestamo"}


def op_renovacion(db: dict, payload: dict):
    codigo = payload.get("libro_codigo")
    usuario_id = payload.get("usuario_id")
    libro = find_libro(db, codigo)
    if not libro: return db, {"ok": False}

    for p in libro["prestamos"]:
        if p["usuario_id"] == usuario_id:
            if p["renovaciones"] >= 2:
                return db, {"ok": False, "mensaje": "Max renovaciones"}
            p["renovaciones"] += 1
            incrementar_version(db)  # IMPORTANTE
            return db, {"ok": True, "mensaje": "Renovado"}

    return db, {"ok": False}


# SINCRONIZACIÓN ENTRE SEDES
def monitor_peer_and_sync(local_sede: int, local_bd_path: Path, peer_bd_path: Path):
    """
    Hilo que escucha el heartbeat de la OTRA sede.
    Si peer.version > local.version -> COPIAR BD del peer a local.
    """
    global db_in_memory

    peer_sede = 2 if local_sede == 1 else 1
    if local_sede == 1:
        peer_hb_endpoint = GA_SEDE2_HEARTBEAT_ENDPOINT
    else:
        peer_hb_endpoint = GA_SEDE1_HEARTBEAT_ENDPOINT

    print(f"[Sync] Monitoreando a Sede {peer_sede} en {peer_hb_endpoint}")

    ctx = zmq.Context()
    sub = create_sub_socket(ctx, peer_hb_endpoint, b"HEARTBEAT")

    while True:
        try:
            # Escuchamos heartbeat
            _, raw = sub.recv_multipart()
            msg = json.loads(raw.decode())

            peer_version = msg.get("version", -1)

            # Chequeo seguro con Lock
            with db_lock:
                local_version = db_in_memory.get("version", 0)

            if peer_version > local_version:
                print(
                    f"[Sync] ALERTA: Mi version ({local_version}) es menor a Sede {peer_sede} ({peer_version}). Sincronizando...")

                # 1. Bloquear DB local
                with db_lock:
                    # 2. Copiar archivo físico de la otra sede a la mía
                    # (En un sistema real esto sería transferencia de archivo por red,
                    # aquí aprovechamos que están en el mismo FS).
                    try:
                        shutil.copy2(peer_bd_path, local_bd_path)
                        print(f"[Sync] Archivo copiado de {peer_bd_path} a {local_bd_path}")

                        # 3. Recargar en memoria
                        db_in_memory = load_db(local_bd_path)
                        print(f"[Sync] BD Recargada. Nueva version local: {db_in_memory['version']}")
                    except Exception as e:
                        print(f"[Sync] ERROR CRÍTICO al copiar BD: {e}")

        except Exception as e:
            # print(f"[Sync] Esperando peer... ({e})")
            time.sleep(1)


def async_save_replica(replica_path: Path, db: dict):
    try:
        db_copy = json.loads(json.dumps(db))
        t = threading.Thread(target=save_db, args=(replica_path, db_copy), daemon=True)
        t.start()
    except:
        pass


# LOOP PRINCIPAL
def run_ga(sede: int):
    global db_in_memory

    paths = get_bd_paths_for_sede(sede)
    primaria = Path(paths["primaria"])
    replica = Path(paths["replica"])

    # Rutas para sincronización (necesitamos saber donde está la BD del vecino)
    other_sede = 2 if sede == 1 else 1
    other_paths = get_bd_paths_for_sede(other_sede)
    peer_primaria = Path(other_paths["primaria"])

    ensure_initial_data(primaria)

    # Cargar BD inicial
    with db_lock:
        try:
            db_in_memory = load_db(primaria)
        except:
            db_in_memory = load_db(replica)
            save_db(primaria, db_in_memory)

    print(f"[GA Sede {sede}] BD Cargada. Version: {db_in_memory.get('version', 0)}")

    # Configurar endpoints
    if sede == 1:
        endpoint = GA_SEDE1_ENDPOINT
        hb_endpoint = GA_SEDE1_HEARTBEAT_ENDPOINT
    else:
        endpoint = GA_SEDE2_ENDPOINT
        hb_endpoint = GA_SEDE2_HEARTBEAT_ENDPOINT

    # Callback para el Heartbeat: entregar versión actual
    def get_ga_status():
        with db_lock:
            v = db_in_memory.get("version", 0)
        return {"version": v, "estado": "OK"}

    # 1. Iniciar Heartbeat Emisor (dice "estoy vivo y esta es mi version")
    start_ga_heartbeat(hb_endpoint, sede, get_ga_status, interval=1.0)

    # 2. Iniciar Monitor de Sincronización (escucha al vecino y se actualiza si es necesario)
    t_sync = threading.Thread(
        target=monitor_peer_and_sync,
        args=(sede, primaria, peer_primaria),
        daemon=True
    )
    t_sync.start()

    # 3. Iniciar Servidor de Peticiones (REQ/REP)
    context = create_context()
    socket_rep = create_rep_socket(context, endpoint)
    print(f"[GA Sede {sede}] Listo para peticiones en {endpoint}")

    while True:
        raw = socket_rep.recv()
        msg = decode_message(raw)

        operacion = msg.get("operacion")
        payload = msg.get("payload", {}) or {}

        respuesta = {}

        # Bloqueamos la BD mientras procesamos para evitar conflictos con la sincronización
        with db_lock:
            # Si estamos muy desactualizados, podríamos rechazar peticiones,
            # pero el hilo de sync lo arreglará rápido.

            if operacion == "prestamo":
                db_in_memory, respuesta = op_prestamo(db_in_memory, payload)
            elif operacion == "devolucion":
                db_in_memory, respuesta = op_devolucion(db_in_memory, payload)
            elif operacion == "renovacion":
                db_in_memory, respuesta = op_renovacion(db_in_memory, payload)
            else:
                respuesta = {"ok": False, "mensaje": "Op desconocida"}

            # Guardar cambios
            save_db(primaria, db_in_memory)
            async_save_replica(replica, db_in_memory)

        socket_rep.send(encode_message(respuesta))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sede", type=int, required=True)
    args = parser.parse_args()
    run_ga(args.sede)