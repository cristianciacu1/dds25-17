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

connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_HOST))
channel = connection.channel()

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


def publish_message(message, status, order_id, user_id):
    """Helper function to publish messages to RabbitMQ"""
    response = {
        "message": message.format(order_id=order_id, user_id=user_id),
        "order_id": order_id,
        "status": status,
        "type": "payment"
    }
    channel.basic_publish(
        exchange="",
        routing_key=ORDER_CHECKOUT_SAGA_REPLIES_QUEUE,
        body=json.dumps(response),
    )
    app.logger.info(response["message"])


def process_message(ch, method, properties, body):
    """Callback function to process messages from RabbitMQ queue."""

    channel.queue_declare(queue=ORDER_CHECKOUT_SAGA_REPLIES_QUEUE)

    # Expected message type: {'user_id': int, 'total_cost': int}.
    message = json.loads(body.decode())
    user_id = message["user_id"]
    amount = message["total_cost"]
    order_id = message["order_id"]

    try:
        user_entry: UserValue = get_user_from_db(user_id)
    except HTTPException:
        publish_message(
            "User {user_id} does not exist. Process stops here.", 400, order_id, user_id
        )
        return
    user_entry.credit -= int(amount)
    if user_entry.credit < 0:
        # If the user does not have enough funds, then publish FAIL event to the
        # order checkout saga.
        publish_message(
            f"For order {order_id}, the user {user_id} did not have enough funds.",
            400,
            order_id,
            user_id,
        )
        return
    try:
        # Try to persist the change in user's funds.
        db.set(user_id, msgpack.encode(user_entry))

        # If successful, then publish SUCCESS event to the order checkout saga
        # replies queue.
        publish_message(
            f"For order {order_id}, the user {user_id} was charged successfully.",
            200,
            order_id,
            user_id,
        )
    except redis.exceptions.RedisError:
        # If the change in user's funds could not be persisted, then publish FAIL
        # event to the order checkout saga.
        publish_message(
            f"For order {order_id}, the user {user_id} has enough funds, but the "
            + "change in their funds could not be persisted in the database.",
            400,
            order_id,
            user_id,
        )


def consume_stock_service_requests_queue():
    """Continuously listen for messages on the order events queue."""

    # Ensure the queue exists.
    channel.queue_declare(queue=PAYMENT_SERVICE_REQUESTS_QUEUE)

    # Start consuming messages.
    channel.basic_consume(
        queue=PAYMENT_SERVICE_REQUESTS_QUEUE, on_message_callback=process_message
    )

    app.logger.info("Started listening to payment service requests queue...")
    channel.start_consuming()


# Start RabbitMQ Consumer in a separate thread.
consumer_thread = threading.Thread(
    target=consume_stock_service_requests_queue, daemon=True
)
consumer_thread.start()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
