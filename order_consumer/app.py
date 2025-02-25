import logging
import os
import atexit
import random
import uuid
from collections import defaultdict
import redis
import pika
from msgspec import msgpack, Struct


db: redis.Redis = redis.Redis(
    host=os.environ["REDIS_HOST"],
    port=int(os.environ["REDIS_PORT"]),
    password=os.environ["REDIS_PASSWORD"],
    db=int(os.environ["REDIS_DB"]),
)


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


class OrderConsumer:
    def __init__(self):
        # Establish connection to RabbitMQ
        self.connection = pika.BlockingConnection(
            pika.URLParameters(os.environ["RABBITMQ_URL"])
        )
        self.channel = self.connection.channel()

        # Declare the queue
        self.channel.queue_declare(queue="order_queue")

        print("OrderConsumer is ready. Waiting for messages...")

    def callback(self, ch, method, properties, body):
        """Processes request and sends response back."""
        msg = msgpack.decode(body)
        try:
            if msg["function"] == "create_order":
                user_id = msg["user_id"]
                key = create_order_db(user_id)
                self.publish_reply(properties, {"order_id": key})
            elif msg["function"] == "batch_init_users":
                kv_pairs = msg["kv_pairs"]
                batch_init_users_db(kv_pairs)
                self.publish_reply(properties, {"msg": "Batch init for orders successful"})
            elif msg["function"] == "find_order":
                order_id = msg["order_id"]
                entry = find_order_db(order_id)
                response = {
                        "order_id": order_id,
                        "paid": entry.paid,
                        "items": entry.items,
                        "user_id": entry.user_id,
                        "total_cost": entry.total_cost
                    }
                self.publish_reply(properties, response)
            elif msg["function"] == "add_item":
                order_entry = msg["order_entry"]
                order_id = msg["order_id"]
                add_item_db(order_id, order_entry)
                self.publish_reply(properties, {"order_id": order_id})
            elif msg["function"] == "checkout":
                order_entry = msg["order_entry"]
                order_id = msg["order_id"]
                checkout_db(order_id, order_entry)
                self.publish_reply(properties, {"msg": "Checkout successful"})
        except Exception as e:
            self.publish_reply(properties, {"status": 400, "msg": "Database error"})
        ch.basic_ack(delivery_tag=method.delivery_tag)




    def start(self):
        self.__init__()
        """Starts consuming messages from RabbitMQ."""
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue="order_queue", on_message_callback=self.callback
        )
        self.channel.start_consuming()

    def publish_reply(self, properties, response):
        self.channel.basic_publish(
            exchange="",
            routing_key=properties.reply_to,  # Reply to the original sender
            properties=pika.BasicProperties(correlation_id=properties.correlation_id),
            body=msgpack.encode(response),
        )


def get_order_from_db(order_id: str) -> OrderValue | None:
    try:
        # get serialized data
        entry: bytes = db.get(order_id)
    except redis.exceptions.RedisError:
        raise Exception
    # deserialize data if it exists else return null
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    return entry


def create_order_db(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(
        OrderValue(paid=False, items=[], user_id=user_id, total_cost=0)
    )
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        raise Exception
    return key


def batch_init_users_db(kv_pairs: dict[str, bytes]):
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        raise Exception


def find_order_db(order_id: str):
    return get_order_from_db(order_id)


def add_item_db(order_id: str, order_entry: OrderValue):
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        raise Exception


def checkout_db(order_id: str, order_entry: OrderValue):
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        raise Exception


if __name__ == "__main__":
    consumer = OrderConsumer()
    while True:
        consumer.start()
