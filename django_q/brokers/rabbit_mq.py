import pika

from django_q.brokers import Broker
from django_q.conf import Conf, logger


class RabbitMQ(Broker):
    def __init__(self, list_key=Conf.PREFIX):
        super(RabbitMQ, self).__init__(list_key="django_q:{}:q".format(list_key))

    def enqueue(self, task):
        return self.connection.basic_publish(
            exchange="", routing_key=self.list_key, body=task
        )

    #        return self.connection.rpush(self.list_key, task)

    def dequeue(self):
        task = self.connection.blpop(self.list_key, 1)
        if task:
            return [(None, task[1])]

    def queue_size(self):
        return self.connection.llen(self.list_key)

    def delete_queue(self):
        return self.connection.delete(self.list_key)

    def purge_queue(self):
        return self.queue_purge(queue=Conf.get("QUEUE_NAME", "djangoq"))
        #return self.connection.ltrim(self.list_key, 1, 0)

    def ping(self):
        return True

    def info(self):
        if not self._info:
            info = self.connection.info("server")
            self._info = "Redis {}".format(info["redis_version"])
        return self._info

    def set_stat(self, key, value, timeout):
        self.connection.set(key, value, timeout)

    def get_stat(self, key):
        if self.connection.exists(key):
            return self.connection.get(key)

    def get_stats(self, pattern):
        keys = self.connection.keys(pattern=pattern)
        if keys:
            return self.connection.mget(keys)

    @staticmethod
    def get_connection(list_key=Conf.PREFIX):
        #        if django_rabbit and Conf.DJANGO_RABBIT:
        #           return django_rabbit.get_redis_connection(Conf.DJANGO_RABBIT)
        #        return redis.StrictRedis(**Conf.REDIS)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(Conf.RABBITMQ)
        )
        channel = connection.channel()
        channel.queue_declare(queue=Conf.get("RABBITMQ_QUEUE_NAME", "djangoq"))
        return channel
