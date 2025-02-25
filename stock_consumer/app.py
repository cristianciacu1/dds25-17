import logging
import os
import atexit
import uuid
import redis
from msgspec import msgpack, Struct
import pika


DB_ERROR_STR = "DB error"


db: redis.Redis = redis.Redis(
    host=os.environ["REDIS_HOST"],
    port=int(os.environ["REDIS_PORT"]),
    password=os.environ["REDIS_PASSWORD"],
    db=int(os.environ["REDIS_DB"]),
)


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class StockValue(Struct):
    stock: int
    price: int

class StockConsumer:
    def __init__(self):
        # Establish connection to RabbitMQ
        self.connection = pika.BlockingConnection(
            pika.URLParameters(os.environ["RABBITMQ_URL"])
        )
        self.channel = self.connection.channel()

        # Declare the queue
        self.channel.queue_declare(queue="stock_queue")

    def callback(self, ch, method, properties, body):
        """Processes request and sends response back."""
        msg = msgpack.decode(body)
        try:
            if msg["function"] == "create_item":
                price = msg["price"]
                key = create_item_db(price)
                self.publish_reply(properties, {"item_id": key})
            elif msg["function"] == "batch_init_users":
                kv_pairs = msg["kv_pairs"]
                batch_init_users_db(kv_pairs)
                self.publish_reply(properties, {"msg": "Batch init for stock successful"})
            elif msg["function"] == "find_item":
                item_id = msg["item_id"]
                item_entry = find_item_db(item_id)
                response = {
                    "stock": item_entry.stock, 
                    "price": item_entry.price
                }
                self.publish_reply(properties, {"status": 200, "entry": response})
            elif msg["function"] == "add_stock":
                item_id = msg["item_id"]
                amount = msg["amount"]
                item_entry = add_stock_db(item_id, amount)
                self.publish_reply(properties, f"Item: {item_id} stock updated to: {item_entry.stock}")
            elif msg["function"] == "remove_stock":
                item_id = msg["item_id"]
                amount = msg["amount"]
                item_entry = remove_stock_db(item_id, amount)
                self.publish_reply(properties, {"status": 200, "msg": f"Item: {item_id} stock updated to: {item_entry.stock}"})
        except Exception as e:
            self.publish_reply(properties, {"status": 400, "msg": "Database error"})
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def start(self):
        self.__init__()
        """Starts consuming messages from RabbitMQ."""
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue="stock_queue", on_message_callback=self.callback
        )
        self.channel.start_consuming()

    def publish_reply(self, properties, response):
        self.channel.basic_publish(
            exchange="",
            routing_key=properties.reply_to,  # Reply to the original sender
            properties=pika.BasicProperties(correlation_id=properties.correlation_id),
            body=msgpack.encode(response),
        )

def get_item_from_db(item_id: str) -> StockValue | None:
    # get serialized data
    try:
        entry: bytes = db.get(item_id)
    except redis.exceptions.RedisError:
        raise Exception
    # deserialize data if it exists else return null
    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    if entry is None:
        # if item does not exist in the database; abort
        raise Exception
    return entry


def create_item_db(price: int):
    key = str(uuid.uuid4())
    value = msgpack.encode(StockValue(stock=0, price=int(price)))
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


def find_item_db(item_id: str):
    return get_item_from_db(item_id)
    

def add_stock_db(item_id: str, amount: int):
    item_entry: StockValue = get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock += int(amount)
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        raise Exception
    return item_entry

def remove_stock_db(item_id: str, amount: int):
    item_entry: StockValue = get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock -= int(amount)
    if item_entry.stock < 0:
       raise Exception
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        raise Exception
    return item_entry

if __name__ == "__main__":
    consumer = StockConsumer()
    while True:
        consumer.start()
