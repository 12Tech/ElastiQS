import json
import time
import threading

from elastiqs import queue_object_factory


def main():

    queue = queue_object_factory(QueueName='elasticQueue')
    for _ in range(10000):
        threading.Thread(target=lambda:queue.send_message(MessageBody=json.dumps({"value": _})) ).start()
        time.sleep(0.01)



if __name__ == "__main__":
    main()