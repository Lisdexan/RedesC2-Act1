# Redes C2
## Actividad 1
Para ejecutar el proyecto se debe runear desde una virtual box, linux.
Usando dos terminales en la carpeta con los comandos: `python3 serverF.py localhost 8000 > recibido.txt` y luego `python3 clienteF.py localhost 8000 < archivo.txt`.

El cliente envía archivo.txt usando SocketTCP (Stop & Wait), y el servidor lo guarda en recibido.txt. Para revisar que llegó íntegro se usa `cmp archivo.txt recibido.txt` (si no imprime nada, es idéntico).

Para probar con pérdidas se usa netem: `sudo tc qdisc add dev lo root netem loss 20.0% delay 0.5s`, y al terminar se desactiva con `sudo tc qdisc del dev lo root netem`.

Se necesita tener SocketTCP.py y archivo.txt en la misma carpeta donde se encuentran serverF.py y clienteF.py
