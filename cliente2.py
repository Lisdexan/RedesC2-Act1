import SocketTCP

address = ("localhost", 9000)

# CLIENT
client_socketTCP = SocketTCP.SocketTCP()
client_socketTCP.connect(address)
# test 1
message = "Mensje de len=16".encode()
client_socketTCP.send(message)
# test 2
message = "Mensaje de largo 19".encode()
client_socketTCP.send(message)
# test 3
message = "Mensaje de largo 19".encode()
client_socketTCP.send(message)

#cierrelul
client_socketTCP.close()
print("Client: conexión cerrada, socket cerrado:", client_socketTCP.sock.fileno() == -1)