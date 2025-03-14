import logging
import os
import atexit
import random
import uuid
from collections import defaultdict
from enum import Enum

import json
import pika
import redis
import requests
import time

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response


DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ["GATEWAY_URL"]
STOCK_SERVICE_REQUESTS_QUEUE = os.environ["STOCK_SERVICE_REQUESTS_QUEUE"]
PAYMENT_SERVICE_REQUESTS_QUEUE = os.environ["PAYMENT_SERVICE_REQUESTS_QUEUE"]
ORDER_CHECKOUT_SAGA_REPLIES_QUEUE = os.environ["ORDER_CHECKOUT_SAGA_REPLIES_QUEUE"]
RABBITMQ_HOST = os.environ["RABBITMQ_URL"]

app = Flask("order-service")

db: redis.Redis = redis.Redis(
    host=os.environ["REDIS_HOST"],
    port=int(os.environ["REDIS_PORT"]),
    password=os.environ["REDIS_PASSWORD"],
    db=int(os.environ["REDIS_DB"]),
)


def close_db_connection():
    db.close()


class OrderValue(Struct):
    stock_status: int
    payment_status: int
    order_status: int
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


class Status(Enum):
    IDLE = 0
    PENDING = 1
    ACCEPTED = 2
    REJECTED = 3


class RabbitMQHandler:
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_HOST))
        self.channel = self.connection.channel()

        result = self.channel.queue_declare(queue="", exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue, on_message_callback=self.callback, auto_ack=True
        )

        self.response = None
        self.corr_id = None

    def callback(self, ch, method, properties, body):
        """Callback function to process messages from RabbitMQ queue."""
        if self.corr_id == properties.correlation_id:
            self.response = json.loads(body.decode())

    def call(self, queue, request_body):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange="",
            routing_key=queue,
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=json.dumps(request_body),
        )
        while self.response is None:
            self.connection.process_data_events(time_limit=None)
        return self.response

    def close_connection(self):
        self.connection.close()


def get_order_from_db(order_id: str) -> OrderValue | None:
    try:
        # get serialized data
        entry: bytes = db.get(order_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    if entry is None:
        # if order does not exist in the database; abort
        abort(400, f"Order: {order_id} not found!")
    return entry


def send_post_request(url: str):
    try:
        response = requests.post(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


def send_get_request(url: str):
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


def rollback_stock(removed_items: list[tuple[str, int]]):
    for item_id, quantity in removed_items:
        send_post_request(f"{GATEWAY_URL}/stock/add/{item_id}/{quantity}")


def rollback_stock_async(order_id: str, items: list[tuple[str, int]]):
    """Publish a rollback stock event to the Stock Service Queue."""
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in items:
        items_quantities[item_id] -= quantity

    stock_service_message = {
        "items": items_quantities,
        "order_id": order_id,
        "type": "compensation",
    }
    rabbitmq_handler.channel.basic_publish(
        exchange="",
        routing_key=STOCK_SERVICE_REQUESTS_QUEUE,
        body=json.dumps(stock_service_message),
    )
    app.logger.debug(
        f"For order {order_id}, rollback stock action pushed to the Stock Service."
    )


def rollback_payment_async(order_id: str, order_entry: OrderValue):
    """Publish refund user event to the Payment Service Queue."""
    payment_service_message = {
        "user_id": order_entry.user_id,
        "total_cost": -order_entry.total_cost,
        "order_id": order_id,
        "type": "compensation",
    }
    rabbitmq_handler.channel.basic_publish(
        exchange="",
        routing_key=PAYMENT_SERVICE_REQUESTS_QUEUE,
        body=json.dumps(payment_service_message),
    )
    app.logger.debug(
        f"For order {order_id}, refund user action pushed to the Payment Service."
    )


# Create a single instance of RabbitMQHandler
rabbitmq_handler = RabbitMQHandler()


@app.post("/create/<user_id>")
def create_order(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(
        OrderValue(
            stock_status=Status.IDLE.value,
            payment_status=Status.IDLE.value,
            order_status=Status.IDLE.value,
            paid=False,
            items=[],
            user_id=user_id,
            total_cost=0,
        )
    )
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"order_id": key})


@app.post("/batch_init/<n>/<n_items>/<n_users>/<item_price>")
def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):

    n = int(n)
    n_items = int(n_items)
    n_users = int(n_users)
    item_price = int(item_price)

    def generate_entry() -> OrderValue:
        user_id = random.randint(0, n_users - 1)
        item1_id = random.randint(0, n_items - 1)
        item2_id = random.randint(0, n_items - 1)
        value = OrderValue(
            stock_status=Status.IDLE.value,
            payment_status=Status.IDLE.value,
            order_status=Status.IDLE.value,
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
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for orders successful"})


@app.get("/find/<order_id>")
def find_order(order_id: str):
    order_entry: OrderValue = get_order_from_db(order_id)
    return jsonify(
        {
            "order_id": order_id,
            "stock_status": order_entry.stock_status,
            "payment_status": order_entry.payment_status,
            "order_status": order_entry.order_status,
            "paid": order_entry.paid,
            "items": order_entry.items,
            "user_id": order_entry.user_id,
            "total_cost": order_entry.total_cost,
        }
    )


@app.post("/addItem/<order_id>/<item_id>/<quantity>")
def add_item(order_id: str, item_id: str, quantity: int):
    order_entry: OrderValue = get_order_from_db(order_id)
    # TODO: Maybe transform this HTTP request to a synchronous RPC call.
    item_reply = send_get_request(f"{GATEWAY_URL}/stock/find/{item_id}")
    if item_reply.status_code != 200:
        # Request failed because item does not exist
        abort(400, f"Item: {item_id} does not exist!")
    item_json: dict = item_reply.json()
    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * item_json["price"]
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(
        f"Item: {item_id} added to: {order_id} "
        f"price updated to: {order_entry.total_cost}",
        status=200,
    )


@app.post("/synccheckout/<order_id>")
def syncCheckout(order_id: str):
    app.logger.debug(f"Checking out {order_id}")
    order_entry: OrderValue = get_order_from_db(order_id)
    # get the quantity per item
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity
    # The removed items will contain the items that we already have successfully
    # subtracted stock from for rollback purposes.
    removed_items: list[tuple[str, int]] = []
    for item_id, quantity in items_quantities.items():
        stock_reply = send_post_request(
            f"{GATEWAY_URL}/stock/subtract/{item_id}/{quantity}"
        )
        if stock_reply.status_code != 200:
            # If one item does not have enough stock we need to rollback
            rollback_stock(removed_items)
            abort(400, f"Out of stock on item_id: {item_id}")
        removed_items.append((item_id, quantity))
    user_reply = send_post_request(
        f"{GATEWAY_URL}/payment/pay/{order_entry.user_id}/{order_entry.total_cost}"
    )
    if user_reply.status_code != 200:
        # If the user does not have enough credit we need to rollback all the item
        # stock subtractions.
        rollback_stock(removed_items)
        abort(400, "User out of credit")
    order_entry.paid = True
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    app.logger.debug("Checkout successful")
    return Response("Checkout successful", status=200)


@app.post("/checkout/<order_id>")
def checkout(order_id: str):
    start_time = time.time()
    app.logger.debug(f"Checking out {order_id}.")
    order_entry: OrderValue = get_order_from_db(order_id)

    # If this order was already checked out (either in progress or completed), we
    # should avoid checking it out again.
    # For debug purposes only, comment the following if statement to truly test the performance of the system.
    # if order_entry.order_status != Status.IDLE.value:
    #     app.logger.debug(
    #         f"The process of checking out order {order_id} has already started. This "
    #         + "request is aborted."
    #     )
    #     return Response(
    #         f"The process of checking out order {order_id} has already started. This "
    #         + "request is aborted.",
    #         status=200,
    #     )

    # Update the status of all three steps to PENDING, indicating that the checkout
    # procedure was initiated.
    order_entry.stock_status = Status.PENDING.value
    order_entry.order_status = Status.PENDING.value
    order_entry.payment_status = Status.PENDING.value
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)

    # get the quantity per item.
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity

    # Send subtract stock event to the Stock Service via RPC and wait until it replies.
    stock_service_message = {
        "order_id": order_id,
        "items": items_quantities,
        "type": "action",
    }
    stock_service_response = rabbitmq_handler.call(
        STOCK_SERVICE_REQUESTS_QUEUE, stock_service_message
    )
    app.logger.debug(
        f"Stock Service replied to the subtract stock action for order {order_id}."
    )

    # Send charge user event to the Payment Service via RPC and wait until it replies.
    payment_service_message = {
        "user_id": order_entry.user_id,
        "total_cost": order_entry.total_cost,
        "order_id": order_id,
        "type": "action",
    }
    payment_service_response = rabbitmq_handler.call(
        PAYMENT_SERVICE_REQUESTS_QUEUE, payment_service_message
    )
    app.logger.debug(
        f"Payment Service replied to the charge user action for order {order_id}."
    )

    # Order Checkout Saga.
    response_status_from_stock_service = stock_service_response["status"]
    response_status_from_payment_service = payment_service_response["status"]

    if (
        response_status_from_stock_service == 200
        and response_status_from_payment_service == 200
    ):
        order_entry.stock_status = Status.ACCEPTED.value
        order_entry.payment_status = Status.ACCEPTED.value
        order_entry.order_status = Status.ACCEPTED.value
        app.logger.debug(f"Order {order_id} was checked out successfully.")
    else:
        order_entry.stock_status = Status.REJECTED.value
        order_entry.payment_status = Status.REJECTED.value
        order_entry.order_status = Status.REJECTED.value
        if response_status_from_stock_service == 200:
            # Stock update was successful, but payment has failed.
            # Rollback the stock action.
            rollback_stock_async(order_id, order_entry.items)
        if response_status_from_payment_service == 200:
            # Payment was successful, but stock update has failed. Refund the user.
            rollback_payment_async(order_id, order_entry)
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)

    end_time = time.time()
    execution_time = end_time - start_time
    app.logger.debug(
        f"Checkout for order {order_id} took {execution_time:.6f} seconds."
    )

    if order_entry.order_status == Status.ACCEPTED.value:
        return Response(
            f"Order {order_id} was checked out successfully.",
            status=200,
        )
    else:
        return Response(
            f"Order {order_id} was not checked out successfully.",
            status=400,
        )


atexit.register(close_db_connection)
# atexit.register(rabbitmq_handler.close_connection)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
