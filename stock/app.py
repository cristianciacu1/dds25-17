import logging
import os
import atexit
import uuid

import pika
import threading
import redis
import json

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response


DB_ERROR_STR = "DB error"
STOCK_SERVICE_REQUESTS_QUEUE = os.environ["STOCK_SERVICE_REQUESTS_QUEUE"]
ORDER_CHECKOUT_SAGA_REPLIES_QUEUE = os.environ["ORDER_CHECKOUT_SAGA_REPLIES_QUEUE"]
RABBITMQ_HOST = os.environ["RABBITMQ_URL"]

app = Flask("stock-service")

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


class StockValue(Struct):
    stock: int
    price: int


def get_item_from_db(item_id: str) -> StockValue | None:
    # get serialized data
    try:
        entry: bytes = db.get(item_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    if entry is None:
        # if item does not exist in the database; abort
        abort(400, f"Item: {item_id} not found!")
    return entry


@app.post("/item/create/<price>")
def create_item(price: int):
    key = str(uuid.uuid4())
    app.logger.debug(f"Item: {key} created")
    value = msgpack.encode(StockValue(stock=0, price=int(price)))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"item_id": key})


@app.post("/batch_init/<n>/<starting_stock>/<item_price>")
def batch_init_users(n: int, starting_stock: int, item_price: int):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    kv_pairs: dict[str, bytes] = {
        f"{i}": msgpack.encode(StockValue(stock=starting_stock, price=item_price))
        for i in range(n)
    }
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for stock successful"})


@app.get("/find/<item_id>")
def find_item(item_id: str):
    app.logger.info(f"Item {item_id} is searched.")
    item_entry: StockValue = get_item_from_db(item_id)
    return jsonify({"stock": item_entry.stock, "price": item_entry.price})


@app.post("/add/<item_id>/<amount>")
def add_stock(item_id: str, amount: int):
    item_entry: StockValue = get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock += int(amount)
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


@app.post("/subtract/<item_id>/<amount>")
def remove_stock(item_id: str, amount: int):
    item_entry: StockValue = get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock -= int(amount)
    app.logger.debug(f"Item: {item_id} stock updated to: {item_entry.stock}")
    if item_entry.stock < 0:
        abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


def process_message(ch, method, properties, body):
    """Callback function to process messages from RabbitMQ queue."""
    # Expected message type: {'item_id': id, 'quantity': n, ...}.
    message = json.loads(body.decode())

    # The removed items will contain the items that we already have successfully
    # subtracted stock from for rollback purposes.
    removed_items: list[tuple[str, int]] = []
    for item_id, quantity in message.items():
        item_entry: StockValue = get_item_from_db(item_id)
        # update stock, serialize and update database.
        item_entry.stock -= int(quantity)
        if item_entry.stock < 0:
            # Rollback the stock to the already processed items.
            for removed_item_id, removed_quantity in removed_items:
                # Possible optimization: keep the StockValue object in the
                # `removed_items` list.
                removed_item_entry: StockValue = get_item_from_db(removed_item_id)
                removed_item_entry.stock += int(removed_quantity)
                try:
                    db.set(removed_item_id, msgpack.encode(removed_item_entry))
                except redis.exceptions.RedisError as e:
                    # TODO: Handle DB exceptions more carefully.
                    # Publish FAIL message to the Order Checkout saga replies queue.
                    message = {
                        "message": f"For item: '{removed_item_id}', there was a "
                        + "database error when trying to rollback the updated value."
                        + f"\n{e}",
                        "status": 400,
                    }
                    channel.basic_publish(
                        exchange="",
                        routing_key=ORDER_CHECKOUT_SAGA_REPLIES_QUEUE,
                        body=json.dumps(message),
                    )
                    app.logger.warning(
                        f"For item: '{removed_item_id}', there was a database error "
                        + "when trying to rollback the updated value.\n{e}"
                    )
                    return
            channel.queue_declare(queue=ORDER_CHECKOUT_SAGA_REPLIES_QUEUE)
            message = {
                "message": f"For item: '{item_id}', there was not enough stock.",
                "status": 400,
            }
            channel.basic_publish(
                exchange="",
                routing_key=ORDER_CHECKOUT_SAGA_REPLIES_QUEUE,
                body=json.dumps(message),
            )
            app.logger.warning(
                f"For item: '{item_id}', there was not enough stock. Thus, this "
                + "order was no longer processed."
            )
            return
        try:
            removed_items.append((item_id, quantity))
            db.set(item_id, msgpack.encode(item_entry))
            app.logger.debug(f"Item: {item_id} stock updated to: {item_entry.stock}.")
        except redis.exceptions.RedisError as e:
            # Publish FAIL message to the Order Checkout saga replies queue.
            message = {
                "message": f"For item: '{item_id}', there was a database error when "
                + f"trying to save the updated value.\n{e}",
                "status": 400,
            }
            channel.basic_publish(
                exchange="",
                routing_key=ORDER_CHECKOUT_SAGA_REPLIES_QUEUE,
                body=json.dumps(message),
            )
            app.logger.warning(
                f"For item: '{item_id}', there was a database error when trying to "
                + f"save the updated value.\n{e}"
            )
            return

    # Publish SUCCESS message to the Order Checkout saga replies queue.
    message = {
        "message": "Stock was successfully updated based on the order.",
        "status": 200,
    }
    channel.basic_publish(
        exchange="",
        routing_key=ORDER_CHECKOUT_SAGA_REPLIES_QUEUE,
        body=json.dumps(message),
    )
    app.logger.info("Stock was successfully updated based on the order.")


def consume_stock_service_requests_queue():
    """Continuously listen for messages on the order events queue."""

    # Ensure the queue exists.
    channel.queue_declare(queue=STOCK_SERVICE_REQUESTS_QUEUE)

    # Start consuming messages.
    channel.basic_consume(
        queue=STOCK_SERVICE_REQUESTS_QUEUE, on_message_callback=process_message
    )

    app.logger.info("Started listening to stock service requests queue...")
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
