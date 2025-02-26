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


class UserValue(Struct):
    credit: int


class PaymentConsumer:
    def __init__(self):
        # Establish connection to RabbitMQ
        self.connection = pika.BlockingConnection(
            pika.URLParameters(os.environ["RABBITMQ_URL"])
        )
        self.channel = self.connection.channel()

        # Declare the queue
        self.channel.queue_declare(queue="payment_queue")

        print("PaymentConsumer is ready. Waiting for messages...")

    def callback(self, ch, method, properties, body):
        """Processes request and sends response back."""
        msg = msgpack.decode(body)
        try:
            match msg["function"]:
                case "create_user":
                    key = create_user_db()
                    self.publish_reply(properties, {"user_id": key})
                case "batch_init_users":
                    kv_pairs = msg["kv_pairs"]
                    batch_init_users_db(kv_pairs)
                    self.publish_reply(
                        properties, {"msg": "Batch init for users successful"}
                    )
                case "find_user":
                    user_id = msg["user_id"]
                    user_entry = find_user_db(user_id)
                    response = {"user_id": user_id, "credit": user_entry.credit}
                    self.publish_reply(properties, response)
                case "add_credit":
                    user_id = msg["user_id"]
                    amount = msg["amount"]
                    user_entry = add_credit_db(user_id, amount)
                    self.publish_reply(
                        properties,
                        "User: {} credit updated to: {}".format(
                            user_id, user_entry.credit
                        ),
                    )
                case "remove_credit":
                    user_id = msg["user_id"]
                    amount = msg["amount"]
                    user_entry = remove_credit_db(user_id, amount)
                    self.publish_reply(
                        properties,
                        {
                            "status": 200,
                            "msg": (
                                "User: {} credit updated to: {}".format(
                                    user_id, user_entry.credit
                                )
                            ),
                        },
                    )
        except Exception:
            self.publish_reply(properties, {"status": 400, "msg": "Database error"})
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def start(self):
        self.__init__()
        """Starts consuming messages from RabbitMQ."""
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue="payment_queue", on_message_callback=self.callback
        )
        self.channel.start_consuming()

    def publish_reply(self, properties, response):
        self.channel.basic_publish(
            exchange="",
            routing_key=properties.reply_to,  # Reply to the original sender
            properties=pika.BasicProperties(correlation_id=properties.correlation_id),
            body=msgpack.encode(response),
        )


def get_user_from_db(user_id: str) -> UserValue | None:
    try:
        # get serialized data
        entry: bytes = db.get(user_id)
    except redis.exceptions.RedisError:
        raise Exception
    # deserialize data if it exists else return null
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        # if user does not exist in the database; abort
        raise Exception
    return entry


def create_user_db():
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
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


def find_user_db(user_id: str):
    return get_user_from_db(user_id)


def add_credit_db(user_id: str, amount: int):
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit += int(amount)
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        raise Exception
    return user_entry


def remove_credit_db(user_id: str, amount: int):
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit -= int(amount)
    if user_entry.credit < 0:
        raise Exception
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        raise Exception
    return user_entry


if __name__ == "__main__":
    consumer = PaymentConsumer()
    while True:
        consumer.start()
