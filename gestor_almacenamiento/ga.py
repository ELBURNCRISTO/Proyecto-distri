#gestor_almacenamiento/ga.py
import argparse
import json
import random
import threading
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
    encode_message,
    decode_message,
)


#UTILIDADES BD en JSON
def load_db(path: Path) -> dict:
    """Carga la BD desde un archivo JSON.
    Lanza excepción si el archivo no existe o está corrupto.
    """
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def save_db(path: Path, db: dict):
    path.parent.mkdir(exist_ok=True, parents=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(db, f, ensure_ascii=False, indent=2)

def ensure_initial_data(primaria_path: Path):
    """Crea una BD inicial si no existe (1000 libros, 200 prestados)"""
    if primaria_path.exists():
        return

    print(f"[GA] Generando BD inicial en {primaria_path} ...")
    libros = []
    total_libros = 1000
    total_prestados = 200

    for i in range(1, total_libros + 1):
        codigo = f"L{i:04d}"
        titulo = f"Libro {i}"
        autor = f"Autor {((i - 1) % 50) + 1}"
        ejemplares_totales = random.randint(1, 4)
        ejemplares_disponibles = ejemplares_totales
        prestamos = []

        libro = {
            "codigo": codigo,
            "titulo": titulo,
            "autor": autor,
            "ejemplares_totales": ejemplares_totales,
            "ejemplares_disponibles": ejemplares_disponibles,
            "prestamos": prestamos,
        }
        libros.append(libro)

    #200 libros prestados
    hoy = datetime.now().date()
    fecha_entrega = hoy + timedelta(days=14)

    indices_prestados = random.sample(range(len(libros)), total_prestados)
    for j, idx in enumerate(indices_prestados):
        libro = libros[idx]
        if libro["ejemplares_disponibles"] > 0:
            libro["ejemplares_disponibles"] -= 1
            libro["prestamos"].append(
                {
                    "usuario_id": f"U{j + 1:04d}",
                    "fecha_prestamo": str(hoy),
                    "fecha_entrega": str(fecha_entrega),
                    "renovaciones": 0,
                }
            )

    db = {"libros": libros}
    save_db(primaria_path, db)
    print(f"[GA] BD inicial creada con {len(libros)} libros, "
          f"{total_prestados} con préstamo activo.")


def find_libro(db: dict, codigo: str) -> dict | None:
    for libro in db.get("libros", []):
        if libro["codigo"] == codigo:
            return libro
    return None


#OPERACIONES
#PRESTAMO
def op_prestamo(db: dict, payload: dict) -> tuple[dict, dict]:
    """Solicitar préstamo de un libro
    payload:
        - libro_codigo
        - usuario_id
        - fecha_actual (YYYY-MM-DD)
    """
    codigo = payload.get("libro_codigo")
    usuario_id = payload.get("usuario_id")
    fecha_actual = payload.get("fecha_actual")
    dias = 14

    libro = find_libro(db, codigo)
    if not libro:
        return db, {
            "ok": False,
            "razon": "LIBRO_NO_EXISTE",
            "mensaje": f"El libro con codigo {codigo} no existe.",
        }

    if libro["ejemplares_disponibles"] <= 0:
        return db, {
            "ok": False,
            "razon": "SIN_EJEMPLARES",
            "mensaje": f"No hay ejemplares disponibles del libro {codigo}.",
        }

    #Verificar si ya tiene un prestamo de este libro
    for prest in libro.get("prestamos", []):
        if prest["usuario_id"] == usuario_id:
            return db, {
                "ok": False,
                "razon": "YA_TIENE_PRESTAMO",
                "mensaje": (
                    f"El usuario {usuario_id} ya tiene un prestamo activo del libro {codigo}."
                ),
            }

    #Registro prestamo
    hoy = datetime.fromisoformat(fecha_actual).date()
    fecha_entrega = hoy + timedelta(days=dias)

    libro["ejemplares_disponibles"] -= 1
    libro["prestamos"].append(
        {
            "usuario_id": usuario_id,
            "fecha_prestamo": str(hoy),
            "fecha_entrega": str(fecha_entrega),
            "renovaciones": 0,
        }
    )

    return db, {
        "ok": True,
        "mensaje": f"Préstamo otorgado del libro {codigo} al usuario {usuario_id}.",
        "fecha_entrega": str(fecha_entrega),
    }

#DEVOLUCION
def op_devolucion(db: dict, payload: dict) -> tuple[dict, dict]:
    """Devolver un libro
    payload:
        - libro_codigo
        - usuario_id
        - fecha_actual (YYYY-MM-DD)
    """
    codigo = payload.get("libro_codigo")
    usuario_id = payload.get("usuario_id")

    libro = find_libro(db, codigo)
    if not libro:
        return db, {
            "ok": False,
            "razon": "LIBRO_NO_EXISTE",
            "mensaje": f"El libro con código {codigo} no existe.",
        }

    prestamos = libro.get("prestamos", [])

    #buscar prestamos de ese usuario
    for i, prest in enumerate(prestamos):
        if prest["usuario_id"] == usuario_id:
            prestamos.pop(i)
            libro["ejemplares_disponibles"] += 1
            return db, {
                "ok": True,
                "mensaje": f"Libro {codigo} devuelto por {usuario_id}.",
            }

    return db, {
        "ok": False,
        "razon": "NO_TIENE_PRESTAMO",
        "mensaje": f"El usuario {usuario_id} no tiene préstamo activo del libro {codigo}.",
    }


def op_renovacion(db: dict, payload: dict) -> tuple[dict, dict]:
    """Renovar un libro una semana
    payload:
        - libro_codigo
        - usuario_id
        - fecha_actual (YYYY-MM-DD)
    """
    codigo = payload.get("libro_codigo")
    usuario_id = payload.get("usuario_id")
    fecha_actual = payload.get("fecha_actual")

    libro = find_libro(db, codigo)
    if not libro:
        return db, {
            "ok": False,
            "razon": "LIBRO_NO_EXISTE",
            "mensaje": f"El libro con código {codigo} no existe.",
        }

    prestamos = libro.get("prestamos", [])
    for prest in prestamos:
        if prest["usuario_id"] == usuario_id:
            if prest["renovaciones"] >= 2:
                return db, {
                    "ok": False,
                    "razon": "MAX_RENOVACIONES",
                    "mensaje": "El libro ya fue renovado 2 veces.",
                }

            # Actualizar fechas
            hoy = datetime.fromisoformat(fecha_actual).date()
            nueva_fecha_entrega = hoy + timedelta(days=7)
            prest["fecha_entrega"] = str(nueva_fecha_entrega)
            prest["renovaciones"] += 1

            return db, {
                "ok": True,
                "mensaje": f"Renovación exitosa del libro {codigo} para {usuario_id}.",
                "nueva_fecha_entrega": str(nueva_fecha_entrega),
                "renovaciones": prest["renovaciones"],
            }

    return db, {
        "ok": False,
        "razon": "NO_TIENE_PRESTAMO",
        "mensaje": f"El usuario {usuario_id} no tiene prestamo activo del libro {codigo}.",
    }

#REPLICACIÓN----------------------------------------
def async_save_replica(replica_path: Path, db: dict):
    """Guarda la BD en la réplica de forma asíncrona
    """
    db_copy = json.loads(json.dumps(db))  # copia profunda
    t = threading.Thread(
        target=save_db,
        args=(replica_path, db_copy),
        daemon=True,
    )
    t.start()


#LOOP PRINCIPAL DEL GA
def run_ga(sede: int):
    """Ejecuta el GA para la sede dada (1 o 2)
    Protocolo de mensaje (JSON):
    {
        "operacion": "prestamo" | "devolucion" | "renovacion",
        "payload": { ... campos ... }
    }
    """
    bd_paths = get_bd_paths_for_sede(sede)
    primaria = Path(bd_paths["primaria"])
    replica = Path(bd_paths["replica"])

    #CARGA CON RECUPERACIÓN DESDE REPLICA
    if primaria.exists():
        #Caso normal: existe BD primaria
        try:
            db = load_db(primaria)
            print(f"[GA Sede {sede}] BD primaria cargada con {len(db.get('libros', []))} libros.")
        except Exception as e:
            print(f"[GA Sede {sede}] ERROR al leer BD primaria: {e}")
            print(f"[GA Sede {sede}] Intentando recuperar desde BD réplica...")

            if replica.exists():
                try:
                    db = load_db(replica)
                    save_db(primaria, db)  # restaurar primaria
                    print(f"[GA Sede {sede}] BD restaurada desde réplica con {len(db.get('libros', []))} libros.")
                except Exception as e2:
                    print(f"[GA Sede {sede}] ERROR al leer BD réplica: {e2}")
                    print(f"[GA Sede {sede}] Regenerando BD inicial (primaria + réplica).")
                    ensure_initial_data(primaria)
                    db = load_db(primaria)
            else:
                print(f"[GA Sede {sede}] No existe réplica. Regenerando BD inicial.")
                ensure_initial_data(primaria)
                db = load_db(primaria)

    else:
        # No existe primaria
        if replica.exists():
            print(f"[GA Sede {sede}] BD primaria NO existe. Recuperando desde réplica...")
            db = load_db(replica)
            save_db(primaria, db)
            print(f"[GA Sede {sede}] BD restaurada desde réplica con {len(db.get('libros', []))} libros.")
        else:
            # No existe primaria NI réplica: primera ejecución real
            print(f"[GA Sede {sede}] NO existe BD. Generando BD inicial...")
            ensure_initial_data(primaria)
            db = load_db(primaria)
            print(f"[GA Sede {sede}] BD inicial creada con {len(db.get('libros', []))} libros.")


    #Endpoint
    if sede == 1:
        endpoint = GA_SEDE1_ENDPOINT
        hb_endpoint = GA_SEDE1_HEARTBEAT_ENDPOINT
    else:
        endpoint = GA_SEDE2_ENDPOINT
        hb_endpoint = GA_SEDE2_HEARTBEAT_ENDPOINT

    print(f"[GA Sede {sede}] Escuchando en {endpoint} ...")
    print(f"[GA Sede {sede}] Publicando en {hb_endpoint} ...")

    start_ga_heartbeat(hb_endpoint, sede, HEARTBEAT_INTERVAL_SECONDS) #iniciar

    context = create_context()
    socket_rep = create_rep_socket(context, endpoint)

    while True:
        raw = socket_rep.recv()  # bloqueante
        msg = decode_message(raw)
        operacion = msg.get("operacion")
        payload = msg.get("payload", {}) or {}
        if operacion == "prestamo":
            db, respuesta = op_prestamo(db, payload)
        elif operacion == "devolucion":
            db, respuesta = op_devolucion(db, payload)
        elif operacion == "renovacion":
            db, respuesta = op_renovacion(db, payload)
        else:
            respuesta = {
                "ok": False,
                "razon": "OPERACION_DESCONOCIDA",
                "mensaje": f"Operación '{operacion}' no soportada por GA.",
            }
        #Guardar en primaria
        save_db(primaria, db)
        #Replicar asíncrona
        async_save_replica(replica, db)
        # Responder al actor
        socket_rep.send(encode_message(respuesta))


#PUNTO DE ENTRADA CLI
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gestor de Almacenamiento (GA)")
    parser.add_argument(
        "--sede",
        type=int,
        choices=[1, 2],
        required=True,
        help="Número de sede (1 o 2)",
    )
    args = parser.parse_args()
    run_ga(args.sede)
