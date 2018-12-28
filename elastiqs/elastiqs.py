
import logging
import time
import threading
import sys
from collections import deque
import multiprocessing
import logging
from datetime import datetime

import boto3

from exceptions import InvalidQueueError, EmptyProductionQueueError

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger('elastiqs')

def queue_object_factory(QueueName, QueueOwnerAWSAccountId=None):
    try:
        sqs = boto3.resource('sqs')
        queue_creation_args = {
            "QueueName": QueueName
        }
        if QueueOwnerAWSAccountId is not None:
            queue_creation_args.update({
                "QueueOwnerAWSAccountId": QueueOwnerAWSAccountId
            })

        return  sqs.get_queue_by_name(**queue_creation_args)

    except Exception as e:
        print(e)
        raise InvalidQueueError(e)

class MessageConsumer(threading.Thread):
    def __init__(self, consumption_queue, production_queue, consumptions, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.consumption_queue = consumption_queue
        self.production_queue = production_queue
        self.consumptions = consumptions
        self.keep_consuming = True

    def run(self):
        while self.keep_consuming:
            for message in self.receive_messages():
                self.consumptions.append(datetime.utcnow().timestamp())
                self.production_queue.append(message)

        logger.info("Exiting thread")

    def receive_messages(self):
        for message in self.consumption_queue.receive_messages(
            MaxNumberOfMessages=10,
            VisibilityTimeout=300,
            WaitTimeSeconds=20):
            yield message        

class ElastiQS(object):
    
    def __init__(self, QueueName, QueueOwnerAWSAccountId=None, *args, **kwargs):

        super().__init__(*args, **kwargs)
        self.production_queue = deque()
        self.consumption_queue = queue_object_factory(QueueName, QueueOwnerAWSAccountId)
        self.threads = []

        # Stats
        self.productions = deque()
        self.consumptions = deque()

    @property
    def max_threads(self):
        """Max number of threads which will be spawned"""
        return 3 * multiprocessing.cpu_count() + 1

    @property
    def production_rate(self):
        """Productions per time unit
        :return: Number of productions per time unit
        """

        try:
            return len(self.productions) / (self.productions[-1] - self.productions[0])
        except:
            return 0

    @property
    def consumption_rate(self):
        """Consumptions per time unit
        :return: Number of consumptions per time unit
        """
        
        try:
            return len(self.consumptions) / (self.consumptions[-1] - self.consumptions[0])
        except:
            return 0        

    @property
    def throughtput_rate(self):
        try:
            return self.production_rate / self.consumption_rate
        except:
            return 0

    def update_rates(self):
        """Update production/consumption rate"""

        self.productions.append(datetime.utcnow().timestamp())

        # logmsg = "Throughtput rate: {}".format(self.throughtput_rate)
        # logger.info(logmsg)

        logmsg = "Rates: {}/{}/{}".format(self.production_rate, self.consumption_rate, self.throughtput_rate)
        logger.info(logmsg)


        # Optimum rate 0.9 < rate < 0.97
        if self.throughtput_rate < 0.9:
            self.consume_slower()
        if self.throughtput_rate > 0.999:
            self.consume_faster()
        

    def start_consuming(self):
        for _ in range(self.max_threads):
            self.consume_faster()

    def consume_faster(self):
        """Spawn a new thread in order to increase the performance of the consumer
        """
        if len(self.threads) < self.max_threads:
            logger.info("Spawning new thread")
            consuming_thread = MessageConsumer(
                                    self.consumption_queue, 
                                    self.production_queue,
                                    self.consumptions)

            consuming_thread.setDaemon(True)
            consuming_thread.start()

            self.threads.append(consuming_thread)
        else:
            logger.info("I cannot consume faster, sorry")

    def consume_slower(self):
        if len(self.threads) > 1:
            logger.info("Exiting running thread")
            first_thread = self.threads.pop()
            first_thread.keep_consuming = False

    def produce_one(self):
        if len(self.production_queue):
            self.update_rates()
            return self.production_queue.popleft()

        else:
            raise EmptyProductionQueueError()
        

def main():

    q = ElastiQS(QueueName='elasticQueue')
    q.start_consuming()

    while True:
        def produce():
            try:

                message = q.produce_one()
                message.delete()

            except EmptyProductionQueueError:
                # Empty Queue
                pass
            except KeyboardInterrupt:
                if not len(q.threads):
                    logger.info("Nothing else to kill. Exiting.")
                    sys.exit(0)
                else:
                    q.consume_slower()
        threading.Thread(target=produce).start()
        time.sleep(0.01)
        

if __name__ == "__main__":
    main()