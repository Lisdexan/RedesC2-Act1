import socket
import sys


HOST = "127.0.0.1"
PORT = 8000
MAX_CHUNK = 16  # tamaño máximo de payload por datagrama (sin headers, todavía no existen en este paso)


def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((HOST, PORT))
    print(f"[servidor] escuchando en {HOST}:{PORT}", file=sys.stderr)

    while True:
        data, addr = sock.recvfrom(MAX_CHUNK)
        if not data:
            continue
        # Escribimos directo al buffer binario de stdout para no romper
        # contenido que no sea texto plano / ASCII.
        sys.stdout.buffer.write(data)
        sys.stdout.flush()


if __name__ == "__main__":
    main()

#tomas el codigo de la profe no orientado a conexion, le haces clink clink clink y si no te sale se lo das a gpt