import zmq

ctx = zmq.Context()
sock = ctx.socket(zmq.PULL)
sock.bind("tcp://*:5556")
print("🟢 Czekam na wiadomości od MT4 na porcie 5556...")

try:
    while True:
        msg = sock.recv_string()
        print("📩 Odebrano:", msg)
except KeyboardInterrupt:
    print("Zamykam.")
finally:
    sock.close()
    ctx.term()
