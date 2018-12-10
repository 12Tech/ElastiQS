
import logging
import threading
from collections import deque

import boto3

from .exceptions import InvalidQueueError


def compose_queue_object(QueueName, QueueOwnerAWSAccountId=None):
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

    



class ElastiQS(deque):
    
    def __init__(self, QueueName, QueueOwnerAWSAccountId=None, *args, **kwargs):

        super().__init__(*args, **kwargs)
        self.queue = compose_queue_object(QueueName, QueueOwnerAWSAccountId)

def main():

    q = ElastiQS('elasticQueue')
    print(dir(q))


if __name__ == "__main__":
    main()