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

        if msg["function"] == "create_order":
            key = create_order_db(msg["user_id"])
            self.publish_reply(properties, {"status": 200, "order_id": key})

    def start(self):
        """Starts consuming messages from RabbitMQ."""
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


def batch_init_users_db(n: int, n_items: int, n_users: int, item_price: int):

    n = int(n)
    n_items = int(n_items)
    n_users = int(n_users)
    item_price = int(item_price)

    def generate_entry() -> OrderValue:
        user_id = random.randint(0, n_users - 1)
        item1_id = random.randint(0, n_items - 1)
        item2_id = random.randint(0, n_items - 1)
        value = OrderValue(
            paid=False,
            items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
            user_id=f"{user_id}",
            total_cost=2 * item_price,
        )
        return value

    kv_pairs: dict[str, bytes] = {
        f"{i}": msgpack.encode(generate_entry()) for i in range(n)
    }
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
    consumer.start()
