#comun/config.py

import os
from pathlib import Path

#RUTAS
#Carpeta raíz
BASE_DIR = Path(__file__).resolve().parent.parent

#Carpeta de las BD en JSON
BD_DIR = BASE_DIR / "bd"
BD_DIR.mkdir(exist_ok=True)

#Archivos de la BD
BD_PRIMARIA_SEDE1 = BD_DIR / "bd_primaria_sede1.json"
BD_REPLICA_SEDE1 = BD_DIR / "bd_replica_sede1.json"
BD_PRIMARIA_SEDE2 = BD_DIR / "bd_primaria_sede2.json"
BD_REPLICA_SEDE2 = BD_DIR / "bd_replica_sede2.json"

#PUB/SUB ZEROMQ
TOPIC_DEVOLUCION = b"DEVOLUCION"
TOPIC_RENOVACION = b"RENOVACION"

#DIRECCIONES Y PUERTOS ZEROMQ

#Gestores de Carga PS <-> GC
GC_SEDE1_ENDPOINT_REQREP = os.getenv(
    "GC_SEDE1_ENDPOINT_REQREP",
    "tcp://127.0.0.1:5551"  # PS <-> GC Sede 1
)

GC_SEDE2_ENDPOINT_REQREP = os.getenv(
    "GC_SEDE2_ENDPOINT_REQREP",
    "tcp://127.0.0.1:5552"  # PS <-> GC Sede 2
)

#Publicación de eventos GC -> Actores
GC_SEDE1_ENDPOINT_PUB = os.getenv(
    "GC_SEDE1_ENDPOINT_PUB",
    "tcp://127.0.0.1:6001"  #DEVOLUCION/RENOVACION Sede 1
)

GC_SEDE2_ENDPOINT_PUB = os.getenv(
    "GC_SEDE2_ENDPOINT_PUB",
    "tcp://127.0.0.1:6002"  #DEVOLUCION/RENOVACION Sede 2
)

#Comunicación síncrona GC <-> Actor de préstamos
ACTOR_PRESTAMOS_SEDE1_ENDPOINT = os.getenv(
    "ACTOR_PRESTAMOS_SEDE1_ENDPOINT",
    "tcp://127.0.0.1:6101"
)

ACTOR_PRESTAMOS_SEDE2_ENDPOINT = os.getenv(
    "ACTOR_PRESTAMOS_SEDE2_ENDPOINT",
    "tcp://127.0.0.1:6102"
)

#Gestor de Almacenamiento GA
GA_SEDE1_ENDPOINT = os.getenv(
    "GA_SEDE1_ENDPOINT",
    "tcp://127.0.0.1:7001"
)

GA_SEDE2_ENDPOINT = os.getenv(
    "GA_SEDE2_ENDPOINT",
    "tcp://127.0.0.1:7002"
)


#HEARTBEAT TOLERANCIA A FALLOS
#Puertos heartbeat GA para detectar caídas
GA_SEDE1_HEARTBEAT_ENDPOINT = os.getenv(
    "GA_SEDE1_HEARTBEAT_ENDPOINT",
    "tcp://127.0.0.1:8001"
)

GA_SEDE2_HEARTBEAT_ENDPOINT = os.getenv(
    "GA_SEDE2_HEARTBEAT_ENDPOINT",
    "tcp://127.0.0.1:8002"
)

HEARTBEAT_INTERVAL_SECONDS = 2.0    #stoy vivo
HEARTBEAT_TIMEOUT_SECONDS = 5.0



#UTILIDADES
def get_bd_paths_for_sede(sede: int):
    """Devuelve rutas de BD primaria y réplica para una sede dada (1 o 2)."""
    if sede == 1:
        return {
            "primaria": BD_PRIMARIA_SEDE1,
            "replica": BD_REPLICA_SEDE1,
        }
    elif sede == 2:
        return {
            "primaria": BD_PRIMARIA_SEDE2,
            "replica": BD_REPLICA_SEDE2,
        }
    else:
        raise ValueError("La sede debe ser 1 o 2")
