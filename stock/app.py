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
import redis.exceptions


DB_ERROR_STR = "DB error"
STOCK_SERVICE_REQUESTS_QUEUE = os.environ["STOCK_SERVICE_REQUESTS_QUEUE"]
ORDER_CHECKOUT_SAGA_REPLIES_QUEUE = os.environ["ORDER_CHECKOUT_SAGA_REPLIES_QUEUE"]
RABBITMQ_HOST = os.environ["RABBITMQ_URL"]

connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_HOST))
channel = connection.channel()

app = Flask("stock-service")

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


def rollback_stock(order_id: str, removed_items: list[tuple[str, int]]):
    connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_HOST))
    channel = connection.channel()

    """Utility function to rollback all transactions from `removed_items`."""
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
            raise e
    app.logger.info(
        f"For order {order_id}, stock rollback was successful. Rolled back the stock "
        + f"for {len(removed_items)} items."
    )

    connection.close()


def publish_message(message, status, order_id, item_id, e=None):
    channel.queue_declare(queue=ORDER_CHECKOUT_SAGA_REPLIES_QUEUE)

    """Helper function to publish messages to RabbitMQ"""
    response = {
        "message": message.format(order_id=order_id, item_id=item_id, e=e),
        "order_id": order_id,
        "status": status,
        "type": "stock",
    }
    channel.basic_publish(
        exchange="",
        routing_key=ORDER_CHECKOUT_SAGA_REPLIES_QUEUE,
        body=json.dumps(response),
    )
    app.logger.info(response["message"])


def process_message(ch, method, properties, body):
    """Callback function to process messages from RabbitMQ queue."""
    # Expected message type: {'order_id': int, {'item_id': id, 'quantity': n, ...}}.
    message = json.loads(body.decode())
    order_id = message["order_id"]
    items_quantities = message["items_quantities"]

    # The removed items will contain the items that we already have successfully
    # subtracted stock from for rollback purposes.
    removed_items: list[tuple[str, int]] = []
    for item_id, quantity in items_quantities.items():
        item_entry: StockValue = get_item_from_db(item_id)
        # update stock, serialize and update database.
        item_entry.stock -= int(quantity)
        if item_entry.stock < 0:
            try:
                # Rollback the stock to the already processed items.
                rollback_stock(order_id, removed_items)

                # If the rollback was successful, then publish event to the order
                # checkout saga informing it that there was not enough stock for at
                # least one item from the current order.
                publish_message(
                    "For order {order_id}, there was not enough stock for item "
                    + f"{item_id}.",
                    400,
                    order_id,
                    item_id,
                )
                return
            except redis.exceptions.RedisError:
                # If the rollback was not successful, then return early.
                # Order checkout saga was already informed that there was an error.
                return
        try:
            # If we have enough stock for the current `item_id`, then persist the
            # stock subtraction.
            removed_items.append((item_id, quantity))
            db.set(item_id, msgpack.encode(item_entry))
            app.logger.debug(f"Item: {item_id} stock updated to: {item_entry.stock}.")
        except redis.exceptions.RedisError as e:
            # In case there was a database failure, Publish FAIL message to the Order
            # Checkout saga replies queue.
            publish_message(
                "For order {order_id}, there was a database error when "
                + "trying to save the updated value for item {item_id}.\n{e}",
                400,
                order_id,
                item_id,
                e,
            )
            return

    # In case there was enough stock for the entire order, then publish SUCCESS
    # message to the Order Checkout saga replies queue.
    if message["type"] == "action":
        publish_message(
            "For order {order_id}, stock was successfully updated based on the order.",
            200,
            order_id,
            None,
        )
    else:
        # If a rollback was performed, then log the outcome.
        app.logger.info(
            f"For order {order_id}, the stock was rolled back " + "successfully."
        )


def consume_stock_service_requests_queue():
    connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_HOST))
    channel = connection.channel()
    """Continuously listen for messages on the order events queue."""

    # Ensure the queue exists.
    channel.queue_declare(queue=STOCK_SERVICE_REQUESTS_QUEUE)

    # Start consuming messages.
    channel.basic_consume(
        queue=STOCK_SERVICE_REQUESTS_QUEUE,
        on_message_callback=process_message,
        auto_ack=True,
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
