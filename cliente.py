import socket
import sys

HOST = "127.0.0.1"
PORT = 8000
MAX_CHUNK = 16  # tamaño máximo de payload por datagrama


def main():
    ruta = input("Ruta del archivo a enviar: ").strip()

    with open(ruta, "rb") as f:
        contenido = f.read()

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    total_bytes = len(contenido)
    enviado = 0

    for i in range(0, total_bytes, MAX_CHUNK):
        trozo = contenido[i:i + MAX_CHUNK]
        sock.sendto(trozo, (HOST, PORT))
        enviado += len(trozo)

    print(f"[cliente] archivo enviado completo: {enviado}/{total_bytes} bytes", file=sys.stderr)
    sock.close()


if __name__ == "__main__":
    main()