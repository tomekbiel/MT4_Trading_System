# test_port_5556.py
import zmq

def test_bind_5556():
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PULL)
    try:
        sock.bind("tcp://*:5556")
        print("✅ Port 5556 dostępny i działa (Python PULL gotowy na dane od MT4)")
    except zmq.ZMQError as e:
        print(f"❌ Nie można zbindować portu 5556: {e}")
    finally:
        sock.close()
        ctx.term()
