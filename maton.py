import socket
import threading
import time
import json
import sys
import psutil
from pymongo import MongoClient

NODOS = {
    1: "100.78.187.12",
    2: "100.109.255.60",
    3: "100.67.249.101"
}
PUERTO = 5000
ID_LOCAL = int(sys.argv[1]) if len(sys.argv) > 1 else 1
IP_LOCAL = NODOS[ID_LOCAL]

# Cadena de conexión al Replica Set completo con timeout corto para detectar particiones rápido
MONGO_URI = "mongodb://100.78.187.12:27017,100.109.255.60:27017,100.67.249.101:27017/?replicaSet=rs0&serverSelectionTimeoutMS=2500"

class NodoMaton:
    def __init__(self, id_nodo, ip_local):
        self.id = id_nodo
        self.ip = ip_local
        self.estado = 'seguidor'
        self.lider_actual = None
        self.ultimo_latido = time.time()
        self.timeout_latido = 4.0
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.ip, PUERTO))
        self.respuestas_ok = 0
        self.lamport = 0
        self.version_inv = 0
        
        self.mongo = MongoClient(MONGO_URI)
        self.col = self.mongo["taller_sd"]["inventario"]

    def tick_lamport(self, recibido=None):
        if recibido is not None:
            self.lamport = max(self.lamport, recibido) + 1
        else:
            self.lamport += 1
        return self.lamport

    def enviar_mensaje(self, tipo, destino_ip):
        reloj = self.tick_lamport()
        mensaje = json.dumps({"tipo": tipo, "origen": self.id, "lamport": reloj}).encode('utf-8')
        try:
            self.sock.sendto(mensaje, (destino_ip, PUERTO))
        except Exception:
            pass

    def escuchar(self):
        while True:
            datos, addr = self.sock.recvfrom(1024)
            msg = json.loads(datos.decode('utf-8'))
            tipo = msg["tipo"]
            origen = msg["origen"]
            self.tick_lamport(msg.get("lamport", 0))

            if tipo == "ELECTION":
                if origen < self.id:
                    self.enviar_mensaje("OK", addr[0])
                    threading.Thread(target=self.iniciar_eleccion).start()
            elif tipo == "OK":
                self.respuestas_ok += 1
                self.estado = 'seguidor'
            elif tipo == "COORDINATOR":
                self.lider_actual = origen
                self.estado = 'seguidor'
                self.ultimo_latido = time.time()
                print(f"[{self.id}] (L:{self.lamport}) Nodo {origen} es el nuevo LÍDER.")
            elif tipo == "HEARTBEAT":
                if origen >= (self.lider_actual or self.id):
                    if self.estado == 'lider' and origen > self.id:
                        print(f"[{self.id}] (L:{self.lamport}) Detectado líder superior ({origen}). Cediendo liderazgo.")
                        self.estado = 'seguidor'
                    self.lider_actual = origen
                    self.ultimo_latido = time.time()

    def iniciar_eleccion(self):
        self.estado = 'candidato'
        self.respuestas_ok = 0
        nodos_superiores = {id_n: ip for id_n, ip in NODOS.items() if id_n > self.id}
        print(f"[{self.id}] (L:{self.lamport}) Iniciando elección...")
        for ip in nodos_superiores.values():
            self.enviar_mensaje("ELECTION", ip)

        time.sleep(2)
        if self.respuestas_ok == 0 and self.estado == 'candidato':
            self.estado = 'lider'
            self.lider_actual = self.id
            print(f"[{self.id}] (L:{self.lamport}) ¡Gané la elección! Soy el LÍDER.")
            for id_n, ip in NODOS.items():
                if id_n != self.id:
                    self.enviar_mensaje("COORDINATOR", ip)

    def registrar_inventario(self):
        while True:
            if self.estado == 'lider':
                self.version_inv += 1
                reloj = self.tick_lamport()
                doc = {
                    "recurso": "cluster_estado",
                    "lider_id": self.id,
                    "version": self.version_inv,
                    "lamport": reloj,
                    "cpu_pct": psutil.cpu_percent(),
                    "ram_pct": psutil.virtual_memory().percent,
                    "timestamp": time.time()
                }
                try:
                    self.col.insert_one(doc)
                    print(f"[{self.id}] Inventario guardado en MongoDB -> v:{self.version_inv} | Lamport:{reloj}")
                except Exception as e:
                    print(f"[{self.id}] Error BD (Sin quórum/Primario inalcanzable en partición)")
            time.sleep(5)

    def monitor_latidos(self):
        while True:
            if self.estado == 'lider':
                for id_n, ip in NODOS.items():
                    if id_n != self.id:
                        self.enviar_mensaje("HEARTBEAT", ip)
                time.sleep(1.5)
            else:
                if time.time() - self.ultimo_latido > self.timeout_latido:
                    print(f"[{self.id}] Timeout del líder. Iniciando nueva elección.")
                    self.ultimo_latido = time.time()
                    threading.Thread(target=self.iniciar_eleccion).start()
                time.sleep(1)

    def iniciar(self):
        threading.Thread(target=self.escuchar, daemon=True).start()
        threading.Thread(target=self.monitor_latidos, daemon=True).start()
        threading.Thread(target=self.registrar_inventario, daemon=True).start()
        time.sleep(1)
        if self.lider_actual is None:
            self.iniciar_eleccion()
        while True:
            time.sleep(1)

if __name__ == "__main__":
    NodoMaton(ID_LOCAL, IP_LOCAL).iniciar()
