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

app = Flask("stock-service")

db: redis.Redis = redis.Redis(
    host=os.environ["REDIS_HOST"],
    port=int(os.environ["REDIS_PORT"]),
    password=os.environ["REDIS_PASSWORD"],
    db=int(os.environ["REDIS_DB"]),
)


def close_db_connection():
    db.close()


class StockValue(Struct):
    stock: int
    price: int


class RabbitMQHandler:
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_HOST))
        self.channel = self.connection.channel()

        # Declare queues
        self.channel.queue_declare(queue=ORDER_CHECKOUT_SAGA_REPLIES_QUEUE)
        self.channel.queue_declare(queue=STOCK_SERVICE_REQUESTS_QUEUE)

    def callback(self, ch, method, properties, body):
        """Callback function to process messages from RabbitMQ queue."""
        # Expected message type: {'item_id': id, 'quantity': n, ...}.
        message = json.loads(body.decode())
        order_id = message["order_id"]
        order_items_quantities = message["items"]
        order_type = message["type"]

        # The removed items will contain the items that we already have successfully
        # subtracted stock from for rollback purposes.
        removed_items: list[tuple[str, int]] = []
        for item_id, quantity in order_items_quantities.items():
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
                    response_message = (
                        f"For order {order_id}, there was not "
                        + f"enough stock for item {item_id}."
                    )
                    self.publish_message(
                        ORDER_CHECKOUT_SAGA_REPLIES_QUEUE,
                        response_message,
                        400,
                        order_id,
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
                app.logger.debug(
                    f"For order {order_id}, stock of item {item_id} was updated"
                    + f"to: {item_entry.stock}."
                )
            except redis.exceptions.RedisError as e:
                # In case there was a database failure, Publish FAIL message to the
                # Order Checkout saga replies queue.
                response_message = (
                    f"For order {order_id}, there was a database error when "
                    + f"trying to save the updated value for item {item_id}.\n{e}"
                )
                self.publish_message(
                    ORDER_CHECKOUT_SAGA_REPLIES_QUEUE,
                    response_message,
                    400,
                    order_id,
                )
                return

        # In case there was enough stock for the entire order, then publish SUCCESS
        # message to the Order Checkout saga replies queue.
        if order_type == "action":
            # <--------- Do not forget about this when implementing checkout !!!
            response_message = (
                f"For order {order_id}, stock was successfully updated.",
            )
            self.publish_message(
                ORDER_CHECKOUT_SAGA_REPLIES_QUEUE,
                response_message,
                200,
                order_id,
            )
        else:
            # If a rollback was performed, then log the outcome.
            app.logger.debug(
                f"For order {order_id}, the stock was rolled back successfully."
            )

    def publish_message(self, queue, message, status_code, order_id):
        response = {
            "message": message,
            "status": status_code,
            "order_id": order_id,
            "type": "stock",
        }
        self.channel.basic_publish(
            exchange="",
            routing_key=queue,
            body=json.dumps(response),
        )
        app.logger.debug(message)

    def start_consuming(self):
        self.channel.basic_consume(
            queue=STOCK_SERVICE_REQUESTS_QUEUE,
            on_message_callback=self.callback,
            auto_ack=True,
        )
        app.logger.debug("Started listening to stock service requests...")
        self.channel.start_consuming()

    def close_connection(self):
        self.connection.close()


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


def rollback_stock(order_id: str, removed_items: list[tuple[str, int]]):
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
            response_message = f"For order {order_id}, there was a "
            +"database error when trying to rollback the updated value for"
            +f"item {removed_item_id}. \n{e}"
            rabbitmq_handler.publish_message(
                ORDER_CHECKOUT_SAGA_REPLIES_QUEUE,
                response_message,
                400,
                order_id,
            )
            raise e
    app.logger.debug("Stock rollback was successful.")


# Create a single instance of RabbitMQHandler
rabbitmq_handler = RabbitMQHandler()


# Run consumer in a separate thread
def start_consumer():
    rabbitmq_handler.start_consuming()


# Start RabbitMQ Consumer in a separate thread.
consumer_thread = threading.Thread(target=start_consumer, daemon=True)
consumer_thread.start()


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
    app.logger.debug(f"Item {item_id} is searched.")
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


atexit.register(close_db_connection)
atexit.register(rabbitmq_handler.close_connection)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
