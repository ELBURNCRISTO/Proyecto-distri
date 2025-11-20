import subprocess
import time
import sys
import os
import signal
import threading
import random
import json
from pathlib import Path
from comun.zeromq_utils import create_context, create_req_socket, encode_message, decode_message, safe_recv
from comun.config import GC_SEDE1_ENDPOINT_REQREP

# CONFIGURACIÓN
PYTHON_EXE = sys.executable
BASE_DIR = Path(__file__).parent
LOGS_DIR = BASE_DIR / "logs_pruebas"
LOGS_DIR.mkdir(exist_ok=True)

# LISTA DE PROCESOS
procesos = []


def log(msg):
    print(f"[TEST RUNNER] {msg}")


def iniciar_componente(script, args, log_file):
    """Inicia un proceso en segundo plano y redirige su salida a un log"""
    ruta = BASE_DIR / script
    cmd = [PYTHON_EXE, str(ruta)] + args.split()

    f = open(LOGS_DIR / log_file, "w")
    p = subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT)
    procesos.append(p)
    return p, f


def matar_todo():
    """Cierra todos los procesos hijos"""
    log("Cerrando todos los procesos...")
    for p in procesos:
        if p.poll() is None:
            # Enviar SIGTERM (o Kill en Windows)
            p.terminate()
            try:
                p.wait(timeout=2)
            except subprocess.TimeoutExpired:
                p.kill()
    log("Sistema apagado.")


def esperar_inicio(segundos=3):
    log(f"Esperando {segundos}s a que los servicios inicien...")
    time.sleep(segundos)


# --- CLIENTE VIRTUAL PARA PRUEBAS ---
def cliente_virtual(sede, n_peticiones, resultados):
    """Simula un PS enviando muchas peticiones seguidas"""
    context = create_context()
    endpoint = GC_SEDE1_ENDPOINT_REQREP if sede == 1 else "tcp://127.0.0.1:5552"
    socket = create_req_socket(context, endpoint)

    aciertos = 0
    errores = 0
    tiempos = []

    for i in range(n_peticiones):
        t_inicio = time.time()
        # Alternar entre libro existente y aleatorio
        cod_libro = "L0001" if i % 2 == 0 else f"L{random.randint(1, 1000):04d}"

        msg = {
            "operacion": "prestamo",
            "payload": {
                "libro_codigo": cod_libro,
                "usuario_id": f"TEST_{i}",
                "fecha_actual": "2025-11-20"
            }
        }

        try:
            socket.send(encode_message(msg))
            resp = safe_recv(socket)
            if resp and resp.get("ok") is not None:
                aciertos += 1
            else:
                errores += 1
        except:
            errores += 1

        tiempos.append(time.time() - t_inicio)
        # Pequeña pausa para no saturar el socket local del test
        time.sleep(0.05)

    resultados.append({
        "aciertos": aciertos,
        "errores": errores,
        "promedio": sum(tiempos) / len(tiempos) if tiempos else 0
    })


# --- ESCENARIOS DE PRUEBA ---

def test_tolerancia_fallos():
    log("=== INICIANDO TEST: TOLERANCIA A FALLOS ===")

    # 1. Arrancar todo el sistema
    p_ga1, _ = iniciar_componente("gestor_almacenamiento/ga.py", "--sede 1", "ga1.log")
    iniciar_componente("gestor_almacenamiento/ga.py", "--sede 2", "ga2.log")
    iniciar_componente("gestor_carga/gc.py", "--sede 1", "gc1.log")
    iniciar_componente("actores/actor_prestamo.py", "--sede 1", "actor_prestamo1.log")
    # (Opcional: iniciar más actores si se requiere)

    esperar_inicio(5)

    # 2. Matar GA1
    log("¡MATANDO GA SEDE 1! (Simulando fallo)")
    p_ga1.terminate()
    p_ga1.wait()
    time.sleep(2)

    # 3. Enviar petición (Debería funcionar usando GA2)
    log("Enviando petición de préstamo mientras GA1 está muerto...")
    res = []
    cliente_virtual(1, 1, res)

    if res[0]["aciertos"] == 1:
        log(">>> ÉXITO: El sistema procesó la petición usando el respaldo (GA2).")
    else:
        log(">>> FALLO: El sistema no respondió.")

    # 4. Revivir GA1
    log("Reviviendo GA Sede 1...")
    iniciar_componente("gestor_almacenamiento/ga.py", "--sede 1", "ga1_revived.log")
    time.sleep(5)  # Tiempo para que sincronice

    log("Verifica en 'logs_pruebas/ga1_revived.log' si aparece el mensaje de '[Sync]'.")
    matar_todo()


def test_carga_stress():
    log("\n=== INICIANDO TEST: CARGA (MUCHAS PETICIONES) ===")

    # Arrancar sistema limpio
    procesos.clear()  # Limpiar lista
    iniciar_componente("gestor_almacenamiento/ga.py", "--sede 1", "ga1_load.log")
    iniciar_componente("gestor_almacenamiento/ga.py", "--sede 2", "ga2_load.log")
    iniciar_componente("gestor_carga/gc.py", "--sede 1", "gc1_load.log")
    iniciar_componente("actores/actor_prestamo.py", "--sede 1", "actor_prestamo1_load.log")

    esperar_inicio(5)

    # Configuración de carga
    NUM_HILOS = 5  # 5 Clientes simultáneos
    PETICIONES_POR_HILO = 20
    TOTAL = NUM_HILOS * PETICIONES_POR_HILO

    log(f"Lanzando {NUM_HILOS} clientes concurrentes ({PETICIONES_POR_HILO} peticiones c/u)...")

    threads = []
    resultados = []
    start_time = time.time()

    for _ in range(NUM_HILOS):
        t = threading.Thread(target=cliente_virtual, args=(1, PETICIONES_POR_HILO, resultados))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    end_time = time.time()
    duration = end_time - start_time

    total_aciertos = sum(r["aciertos"] for r in resultados)
    total_errores = sum(r["errores"] for r in resultados)
    throughput = TOTAL / duration

    log(f"--- RESULTADOS DE CARGA ---")
    log(f"Tiempo total: {duration:.2f} segundos")
    log(f"Peticiones Totales: {TOTAL}")
    log(f"Exitosas: {total_aciertos} | Fallidas: {total_errores}")
    log(f"Throughput: {throughput:.2f} peticiones/segundo")

    matar_todo()


if __name__ == "__main__":
    try:
        test_tolerancia_fallos()
        time.sleep(2)
        test_carga_stress()
    except KeyboardInterrupt:
        matar_todo()