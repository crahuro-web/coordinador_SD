import json
import os
import threading
import time
import uuid
from typing import Dict, List, Set

import pika
from fastapi import FastAPI
from pydantic import BaseModel
from pymongo import MongoClient

RABBIT_HOST = os.getenv("RABBIT_HOST", "100.78.187.12")
RABBIT_USER = os.getenv("RABBIT_USER", "admin")
RABBIT_PASS = os.getenv("RABBIT_PASS", "admin123")
MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://100.78.187.12:27017,100.109.255.60:27017,100.67.249.101:27017/?replicaSet=rs0&serverSelectionTimeoutMS=3000"
)

app = FastAPI(title="Coordinador SD - Taller 2")

mongo_client = MongoClient(MONGO_URI)
db = mongo_client["taller2_sd"]
col_sagas = db["sagas"]
col_eventos = db["eventos_ordenados"]

# Estado en memoria para reservas, idempotencia y buffer de orden causal
reservas_activas: Dict[str, dict] = {}
eventos_procesados: Set[str] = set()
buffer_eventos: List[dict] = []
buffer_lock = threading.Lock()
reloj_lamport = 0


def tick_lamport(recibido: int = 0) -> int:
    global reloj_lamport
    reloj_lamport = max(reloj_lamport, recibido) + 1
    return reloj_lamport


class MetricaIn(BaseModel):
    nodo_id: int
    cpu_pct: float
    ram_pct: float
    lamport: int


class AsignacionIn(BaseModel):
    nodo_origen: int
    recurso: str
    simular_fallo_paso2: bool = False


@app.get("/health")
def health():
    return {"status": "ok", "lamport": reloj_lamport}


@app.post("/api/v1/metrics")
def recibir_metricas(m: MetricaIn):
    l_actual = tick_lamport(m.lamport)
    print(f"[REST] Métricas Nodo {m.nodo_id} | CPU:{m.cpu_pct}% RAM:{m.ram_pct}% | L:{l_actual}", flush=True)
    return {"status": "confirmado", "lamport": l_actual}


@app.post("/api/v1/assignments")
def ejecutar_saga(req: AsignacionIn):
    saga_id = str(uuid.uuid4())[:8]
    l_actual = tick_lamport()
    registro = {
        "saga_id": saga_id,
        "nodo_origen": req.nodo_origen,
        "recurso": req.recurso,
        "estado": "EN_CURSO",
        "pasos": [],
        "lamport": l_actual,
        "timestamp": time.time(),
    }
    col_sagas.insert_one(registro)
    print(f"[SAGA {saga_id}] INICIADA -> Nodo {req.nodo_origen}, recurso '{req.recurso}'", flush=True)

    # PASO 1: Reservar recurso en nodo origen
    reservas_activas[saga_id] = {"nodo": req.nodo_origen, "recurso": req.recurso}
    col_sagas.update_one({"saga_id": saga_id}, {"$push": {"pasos": "PASO_1_RESERVA_OK"}})
    print(f"[SAGA {saga_id}] Paso 1 OK: Recurso reservado en Nodo {req.nodo_origen}", flush=True)

    # PASO 2: Replicar asignación en MongoDB (o simular falla)
    try:
        if req.simular_fallo_paso2:
            raise RuntimeError("Fallo simulado en Paso 2 (Replicación BD)")
        col_sagas.update_one(
            {"saga_id": saga_id},
            {"$set": {"asignado": True}, "$push": {"pasos": "PASO_2_REPLICACION_OK"}},
        )
        print(f"[SAGA {saga_id}] Paso 2 OK: Asignación replicada en MongoDB rs0", flush=True)
    except Exception as e:
        # COMPENSACIÓN del Paso 1
        reservas_activas.pop(saga_id, None)
        col_sagas.update_one(
            {"saga_id": saga_id},
            {"$set": {"estado": "COMPENSADA", "error": str(e)}, "$push": {"pasos": "COMPENSACION_PASO_1_LIBERAR_RESERVA"}},
        )
        print(f"[SAGA {saga_id}] FALLO Paso 2 ({e}) -> COMPENSADA: Reserva liberada", flush=True)
        return {"saga_id": saga_id, "estado": "COMPENSADA", "detalle": str(e)}

    # PASO 3: Confirmar asignación y notificar
    col_sagas.update_one(
        {"saga_id": saga_id},
        {"$set": {"estado": "COMPLETADA"}, "$push": {"pasos": "PASO_3_CONFIRMADA"}},
    )
    print(f"[SAGA {saga_id}] Paso 3 OK -> COMPLETADA", flush=True)
    return {"saga_id": saga_id, "estado": "COMPLETADA", "lamport": l_actual}


def procesar_buffer_ordenado():
    while True:
        time.sleep(2)
        with buffer_lock:
            if not buffer_eventos:
                continue
            # Orden causal estricto por par (marca_lamport, nodo_id)
            buffer_eventos.sort(key=lambda e: (e["lamport"], e["nodo_id"]))
            lote = list(buffer_eventos)
            buffer_eventos.clear()

        for ev in lote:
            l_actual = tick_lamport(ev["lamport"])
            col_eventos.insert_one(ev)
            print(
                f"[BROKER-ORDENADO] Procesado event_id={ev['event_id']} | "
                f"(L:{ev['lamport']}, Nodo:{ev['nodo_id']}) | tipo={ev['tipo']}",
                flush=True,
            )


def consumidor_rabbitmq():
    creds = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    params = pika.ConnectionParameters(host=RABBIT_HOST, credentials=creds, heartbeat=30)
    while True:
        try:
            conn = pika.BlockingConnection(params)
            ch = conn.channel()
            ch.queue_declare(queue="eventos_sd", durable=True)

            def callback(channel, method, properties, body):
                ev = json.loads(body.decode("utf-8"))
                eid = ev["event_id"]
                if eid in eventos_procesados:
                    print(f"[IDEMPOTENCIA] Evento duplicado descartado: {eid}", flush=True)
                    channel.basic_ack(delivery_tag=method.delivery_tag)
                    return
                eventos_procesados.add(eid)
                with buffer_lock:
                    buffer_eventos.append(ev)
                channel.basic_ack(delivery_tag=method.delivery_tag)

            ch.basic_qos(prefetch_count=10)
            ch.basic_consume(queue="eventos_sd", on_message_callback=callback)
            print("[BROKER] Conectado a RabbitMQ. Escuchando cola 'eventos_sd'...", flush=True)
            ch.start_consuming()
        except Exception as e:
            print(f"[BROKER] Desconectado ({e}). Reintentando en 3s...", flush=True)
            time.sleep(3)


@app.on_event("startup")
def iniciar_hilos():
    threading.Thread(target=consumidor_rabbitmq, daemon=True).start()
    threading.Thread(target=procesar_buffer_ordenado, daemon=True).start()
