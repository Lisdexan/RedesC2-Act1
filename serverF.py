"""Servidor: recibe un archivo por SocketTCP y lo escribe en stdout.

Uso:  python3 servidor.py localhost 8000 > recibido.txt
      python3 servidor.py localhost 8000 --debug > recibido.txt   (debug en stderr)
"""
import sys

import SocketTCP

# recv devuelve min(largo_del_mensaje, buff_size) bytes: con un buff_size grande
# un solo recv entrega el archivo completo.
BUFF_SIZE = 1 << 30


def main():
    args = [a for a in sys.argv[1:] if a != "--debug"]
    if len(args) != 2:
        print("Uso: python3 servidor.py <host> <puerto> [--debug] > salida", file=sys.stderr)
        sys.exit(1)
    host, port = args[0], int(args[1])
    SocketTCP.SocketTCP.DEBUG = "--debug" in sys.argv

    server = SocketTCP.SocketTCP()
    server.bind((host, port))
    connection, _ = server.accept()

    data = connection.recv(BUFF_SIZE)
    sys.stdout.buffer.write(data)
    sys.stdout.buffer.flush()

    connection.recv_close()


if __name__ == "__main__":
    main()