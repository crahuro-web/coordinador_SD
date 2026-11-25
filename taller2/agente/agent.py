import json
import os
import time
import uuid

import pika
import psutil
import requests

NODO_ID = int(os.getenv("NODO_ID", "2"))
COORD_URL = os.getenv("COORD_URL", "http://100.78.187.12:8000")
RABBIT_HOST = os.getenv("RABBIT_HOST", "100.78.187.12")
RABBIT_USER = os.getenv("RABBIT_USER", "admin")
RABBIT_PASS = os.getenv("RABBIT_PASS", "admin123")
AUTO_SAGA = os.getenv("AUTO_SAGA", "true").lower() == "true"

reloj_lamport = 0


def tick_lamport(recibido: int = 0) -> int:
    global reloj_lamport
    reloj_lamport = max(reloj_lamport, recibido) + 1
    return reloj_lamport


def enviar_rest_con_reintentos(endpoint: str, payload: dict, max_reintentos: int = 3):
    espera = 1
    for intento in range(1, max_reintentos + 1):
        try:
            resp = requests.post(f"{COORD_URL}{endpoint}", json=payload, timeout=2.5)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(
                f"[AGENTE {NODO_ID}] Fallo REST {endpoint} (intento {intento}/{max_reintentos}): {e}. "
                f"Reintentando en {espera}s...",
                flush=True,
            )
            time.sleep(espera)
            espera *= 2
    return None


def publicar_evento_rabbitmq(tipo: str, payload: dict, event_id: str = None):
    eid = event_id or str(uuid.uuid4())[:8]
    l_val = tick_lamport()
    mensaje = {
        "event_id": eid,
        "nodo_id": NODO_ID,
        "lamport": l_val,
        "tipo": tipo,
        "payload": payload,
    }
    creds = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    params = pika.ConnectionParameters(host=RABBIT_HOST, credentials=creds, socket_timeout=3)

    espera = 1
    for intento in range(1, 4):
        try:
            conn = pika.BlockingConnection(params)
            ch = conn.channel()
            ch.queue_declare(queue="eventos_sd", durable=True)
            ch.basic_publish(
                exchange="",
                routing_key="eventos_sd",
                body=json.dumps(mensaje),
                properties=pika.BasicProperties(delivery_mode=2),
            )
            conn.close()
            print(
                f"[AGENTE {NODO_ID}] Evento publicado en RabbitMQ -> id={eid} | L:{l_val} | tipo={tipo}",
                flush=True,
            )
            return eid
        except Exception as e:
            print(
                f"[AGENTE {NODO_ID}] Error broker RabbitMQ (intento {intento}/3): {e}. "
                f"Reintentando en {espera}s...",
                flush=True,
            )
            time.sleep(espera)
            espera *= 2
    return None


def ejecutar_flujo_asignacion():
    # 1. Reportar métricas por REST
    l_val = tick_lamport()
    metricas = {
        "nodo_id": NODO_ID,
        "cpu_pct": psutil.cpu_percent(interval=0.5),
        "ram_pct": psutil.virtual_memory().percent,
        "lamport": l_val,
    }
    res_m = enviar_rest_con_reintentos("/api/v1/metrics", metricas)
    if res_m:
        tick_lamport(res_m.get("lamport", 0))
        print(f"[AGENTE {NODO_ID}] Métricas confirmadas por Coordinador -> {res_m}", flush=True)

    # 2. Publicar evento de solicitud en RabbitMQ
    publicar_evento_rabbitmq("ASIGNACION", {"recurso": "gpu_cluster_01", "estado": "solicitado"})

    # 3. Solicitar asignación transaccional (Saga sin fallos)
    solicitud = {
        "nodo_origen": NODO_ID,
        "recurso": "gpu_cluster_01",
        "simular_fallo_paso2": False,
    }
    print(f"[AGENTE {NODO_ID}] Solicitando asignación Saga (sin fallos)...", flush=True)
    res_saga = enviar_rest_con_reintentos("/api/v1/assignments", solicitud)
    if res_saga:
        tick_lamport(res_saga.get("lamport", 0))
        print(f"[AGENTE {NODO_ID}] Resultado Saga recibido -> {res_saga}", flush=True)


if __name__ == "__main__":
    print(f"[AGENTE {NODO_ID}] Iniciando agente contra {COORD_URL}...", flush=True)
    time.sleep(2)
    if AUTO_SAGA:
        ejecutar_flujo_asignacion()

    while True:
        time.sleep(10)
        l_val = tick_lamport()
        metricas = {
            "nodo_id": NODO_ID,
            "cpu_pct": psutil.cpu_percent(interval=0.5),
            "ram_pct": psutil.virtual_memory().percent,
            "lamport": l_val,
        }
        res = enviar_rest_con_reintentos("/api/v1/metrics", metricas)
        if res:
            tick_lamport(res.get("lamport", 0))
            print(f"[AGENTE {NODO_ID}] Latido/Métrica REST OK -> L:{reloj_lamport}", flush=True)
