import atexit
import json
import logging
import os
import pika
import redis
import threading
import uuid

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response
from werkzeug.exceptions import HTTPException

DB_ERROR_STR = "DB error"
PAYMENT_SERVICE_REQUESTS_QUEUE = os.environ["PAYMENT_SERVICE_REQUESTS_QUEUE"]
ORDER_CHECKOUT_SAGA_REPLIES_QUEUE = os.environ["ORDER_CHECKOUT_SAGA_REPLIES_QUEUE"]
RABBITMQ_HOST = os.environ["RABBITMQ_URL"]

app = Flask("payment-service")


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


class RabbitMQHandler:
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_HOST))
        self.channel = self.connection.channel()

        # Declare queues
        self.channel.queue_declare(queue=ORDER_CHECKOUT_SAGA_REPLIES_QUEUE)
        self.channel.queue_declare(queue=PAYMENT_SERVICE_REQUESTS_QUEUE)

    def callback(self, ch, method, properties, body):
        """Callback function to process messages from RabbitMQ queue."""
        # Expected message type: {'user_id': int, 'total_cost': int}.
        message = json.loads(body.decode())
        user_id = message["user_id"]
        amount = message["total_cost"]
        order_id = message["order_id"]

        try:
            user_entry: UserValue = get_user_from_db(user_id)
        except HTTPException:
            self.publish_message(
                ORDER_CHECKOUT_SAGA_REPLIES_QUEUE,
                "User {user_id} does not exist. Process stops here.",
                400,
                order_id,
            )
            return
        user_entry.credit -= int(amount)
        if user_entry.credit < 0:
            # If the user does not have enough funds, then publish FAIL event to the
            # order checkout saga.
            self.publish_message(
                ORDER_CHECKOUT_SAGA_REPLIES_QUEUE,
                f"For order {order_id}, the user {user_id} did not have enough funds.",
                400,
                order_id,
            )
            return
        try:
            # Try to persist the change in user's funds.
            db.set(user_id, msgpack.encode(user_entry))

            # If successful, then publish SUCCESS event to the order checkout saga
            # replies queue.
            self.publish_message(
                ORDER_CHECKOUT_SAGA_REPLIES_QUEUE,
                f"For order {order_id}, the user {user_id} was charged successfully.",
                200,
                order_id,
            )
        except redis.exceptions.RedisError:
            # If the change in user's funds could not be persisted, then publish FAIL
            # event to the order checkout saga.
            self.publish_message(
                ORDER_CHECKOUT_SAGA_REPLIES_QUEUE,
                f"For order {order_id}, the user {user_id} has enough funds, but the "
                + "change in their funds could not be persisted in the database.",
                400,
                order_id,
            )

    def publish_message(self, queue, message, status_code, order_id):
        response = {
            "message": message,
            "status": status_code,
            "order_id": order_id,
            "type": "payment",
        }
        self.channel.basic_publish(
            exchange="",
            routing_key=queue,
            body=json.dumps(response),
        )
        app.logger.debug(message)

    def start_consuming(self):
        self.channel.basic_consume(
            queue=PAYMENT_SERVICE_REQUESTS_QUEUE,
            on_message_callback=self.callback,
            auto_ack=True,
        )
        app.logger.debug("Started listening to stock service requests...")
        self.channel.start_consuming()

    def close_connection(self):
        self.connection.close()


def get_user_from_db(user_id: str) -> UserValue | None:
    try:
        # get serialized data
        entry: bytes = db.get(user_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        # if user does not exist in the database; abort
        abort(400, f"User: {user_id} not found!")
    return entry


# Create a single instance of RabbitMQHandler
rabbitmq_handler = RabbitMQHandler()


# Run consumer in a separate thread
def start_consumer():
    rabbitmq_handler.start_consuming()


# Start RabbitMQ Consumer in a separate thread.
consumer_thread = threading.Thread(target=start_consumer, daemon=True)
consumer_thread.start()


@app.post("/create_user")
def create_user():
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"user_id": key})


@app.post("/batch_init/<n>/<starting_money>")
def batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    kv_pairs: dict[str, bytes] = {
        f"{i}": msgpack.encode(UserValue(credit=starting_money)) for i in range(n)
    }
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for users successful"})


@app.get("/find_user/<user_id>")
def find_user(user_id: str):
    user_entry: UserValue = get_user_from_db(user_id)
    return jsonify({"user_id": user_id, "credit": user_entry.credit})


@app.post("/add_funds/<user_id>/<amount>")
def add_credit(user_id: str, amount: int):
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit += int(amount)
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(
        f"User: {user_id} credit updated to: {user_entry.credit}", status=200
    )


@app.post("/pay/<user_id>/<amount>")
def remove_credit(user_id: str, amount: int):
    app.logger.debug(f"Removing {amount} credit from user: {user_id}")
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit -= int(amount)
    if user_entry.credit < 0:
        abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(
        f"User: {user_id} credit updated to: {user_entry.credit}", status=200
    )


atexit.register(close_db_connection)
atexit.register(rabbitmq_handler.close_connection)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
