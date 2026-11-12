import socket
import threading
import time
import json
import sys

# Configuración de la red Tailscale de los 3 nodos
NODOS = {
    1: "100.78.187.12", # ID menor
    2: "100.109.255.60", 
    3: "100.67.249.101"  # ID mayor (Matón por defecto)
}
PUERTO = 5000
ID_LOCAL = int(sys.argv[1]) if len(sys.argv) > 1 else 1 # Se pasa por consola: python3 maton.py 1
IP_LOCAL = NODOS[ID_LOCAL]

class NodoMaton:
    def __init__(self, id_nodo, ip_local):
        self.id = id_nodo
        self.ip = ip_local
        self.estado = 'seguidor' # 'seguidor', 'candidato', 'lider'
        self.lider_actual = None
        self.ultimo_latido = time.time()
        self.timeout_latido = 4.0
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.ip, PUERTO))
        self.respuestas_ok = 0

    def enviar_mensaje(self, tipo, destino_ip):
        mensaje = json.dumps({"tipo": tipo, "origen": self.id}).encode('utf-8')
        self.sock.sendto(mensaje, (destino_ip, PUERTO))

    def escuchar(self):
        while True:
            datos, addr = self.sock.recvfrom(1024)
            msg = json.loads(datos.decode('utf-8'))
            tipo = msg["tipo"]
            origen = msg["origen"]

            if tipo == "ELECTION":
                # Si recibimos ELECTION de un ID menor, respondemos OK y empezamos nuestra propia elección
                if origen < self.id:
                    self.enviar_mensaje("OK", addr[0])
                    threading.Thread(target=self.iniciar_eleccion).start()
            
            elif tipo == "OK":
                # Un nodo con ID mayor respondió, por lo tanto, no podemos ser líderes
                self.respuestas_ok += 1
                self.estado = 'seguidor'

            elif tipo == "COORDINATOR":
                # Un nodo mayor ganó la elección y se declara líder
                self.lider_actual = origen
                self.estado = 'seguidor'
                self.ultimo_latido = time.time()
                print(f"[{self.id}] Nodo {origen} es el nuevo LÍDER.")

            elif tipo == "HEARTBEAT":
                if origen >= self.lider_actual if self.lider_actual else origen >= self.id:
                    self.lider_actual = origen
                    self.ultimo_latido = time.time()

    def iniciar_eleccion(self):
        self.estado = 'candidato'
        self.respuestas_ok = 0
        nodos_superiores = {id_n: ip for id_n, ip in NODOS.items() if id_n > self.id}
        
        print(f"[{self.id}] Iniciando elección. Contactando IDs superiores...")
        for ip in nodos_superiores.values():
            self.enviar_mensaje("ELECTION", ip)

        # Esperar 2 segundos para ver si algún ID mayor responde con "OK"
        time.sleep(2)

        if self.respuestas_ok == 0 and self.estado == 'candidato':
            # Nadie superior respondió, ¡asumimos el liderazgo!
            self.estado = 'lider'
            self.lider_actual = self.id
            print(f"[{self.id}] ¡Gané la elección! Soy el nuevo LÍDER.")
            # Avisar a todos los demás (IDs menores)
            for id_n, ip in NODOS.items():
                if id_n != self.id:
                    self.enviar_mensaje("COORDINATOR", ip)

    def monitor_latidos(self):
        while True:
            if self.estado == 'lider':
                # El líder envía latidos constantemente
                for id_n, ip in NODOS.items():
                    if id_n != self.id:
                        self.enviar_mensaje("HEARTBEAT", ip)
                time.sleep(1.5)
            else:
                # Los seguidores revisan si el líder sigue vivo
                if time.time() - self.ultimo_latido > self.timeout_latido:
                    print(f"[{self.id}] Timeout del líder. Iniciando nueva elección.")
                    self.ultimo_latido = time.time() # Resetear para no hacer spam
                    threading.Thread(target=self.iniciar_eleccion).start()
                time.sleep(1)

    def iniciar(self):
        threading.Thread(target=self.escuchar, daemon=True).start()
        threading.Thread(target=self.monitor_latidos, daemon=True).start()
        
        # Al iniciar, si no sabemos quién es el líder, forzamos elección
        time.sleep(1)
        if self.lider_actual is None:
            self.iniciar_eleccion()
            
        # Mantener el hilo principal vivo
        while True:
            time.sleep(1)

if __name__ == "__main__":
    nodo = NodoMaton(ID_LOCAL, IP_LOCAL)
    print(f"Iniciando Nodo {ID_LOCAL} en {IP_LOCAL}...")
    nodo.iniciar()
