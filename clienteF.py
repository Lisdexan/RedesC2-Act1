"""Cliente: envía por stdin un archivo al servidor usando SocketTCP.

Uso:  python3 cliente.py localhost 8000 < archivo.txt
      python3 cliente.py localhost 8000 --debug < archivo.txt   (muestra retransmisiones en stderr)
"""
import sys

import SocketTCP


def main():
    args = [a for a in sys.argv[1:] if a != "--debug"]
    if len(args) != 2:
        print("Uso: python3 cliente.py <host> <puerto> [--debug] < archivo", file=sys.stderr)
        sys.exit(1)
    host, port = args[0], int(args[1])
    SocketTCP.SocketTCP.DEBUG = "--debug" in sys.argv

    data = sys.stdin.buffer.read()          # el archivo completo en memoria, como bytes

    client = SocketTCP.SocketTCP()
    client.connect((host, port))
    client.send(data)
    client.close()


if __name__ == "__main__":
    main()