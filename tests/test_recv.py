import zmq

ctx = zmq.Context()
pull = ctx.socket(zmq.PULL)
pull.bind("tcp://*:5599")  # MT4 → Python

print("🟢 Nasłuchuję na 5599 (czy MT4 wysyła coś?)")
while True:
    try:
        msg = pull.recv_string()
        print("📩 Odebrano:", msg)
    except KeyboardInterrupt:
        break
