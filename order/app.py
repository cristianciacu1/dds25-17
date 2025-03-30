import logging
import os
import atexit
import random
import uuid
from collections import defaultdict
from enum import Enum
import socket

import json
import pika
import redis
import requests
import time

from flask import Flask, jsonify, abort, Response
from gevent.pywsgi import WSGIServer
from msgspec import msgpack, Struct


DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ["GATEWAY_URL"]
STOCK_SERVICE_REQUESTS_QUEUE = os.environ["STOCK_SERVICE_REQUESTS_QUEUE"]
PAYMENT_SERVICE_REQUESTS_QUEUE = os.environ["PAYMENT_SERVICE_REQUESTS_QUEUE"]
MESSAGE_TTL = int(os.environ["MESSAGE_TTL"])
DLX_EXCHANGE = os.environ["DLX_EXCHANGE"]
STOCK_DLX_KEY = os.environ["STOCK_DLX_KEY"]
PAYMENT_DLX_KEY = os.environ["PAYMENT_DLX_KEY"]
RABBITMQ_HOST = os.environ["RABBITMQ_URL"]

app = Flask("order-service")
app.logger.setLevel(logging.DEBUG)

db: redis.Redis = redis.Redis(
    host=os.environ["REDIS_HOST"],
    port=int(os.environ["REDIS_PORT"]),
    password=os.environ["REDIS_PASSWORD"],
    db=int(os.environ["REDIS_DB"]),
)

initialize_checkout = """
    redis.call('SET', KEYS[1], ARGV[1])
    redis.call('SET', KEYS[2], ARGV[2])
    return "OK"
"""

finalize_checkout = """
    redis.call('SET', KEYS[1], ARGV[1])
    redis.call('DEL', KEYS[2])
    return "OK"
"""

initialize_checkout = db.register_script(initialize_checkout)
finalize_checkout = db.register_script(finalize_checkout)


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


class LogInfo(Struct):
    order_id: str
    stock_log: str
    stock_rollback_log: str
    payment_log: str
    payment_rollback_log: str


def generate_log_info(order_id: str) -> LogInfo:
    return LogInfo(
        order_id=order_id,
        stock_log=str(uuid.uuid4()),
        stock_rollback_log=str(uuid.uuid4()),
        payment_log=str(uuid.uuid4()),
        payment_rollback_log=str(uuid.uuid4()),
    )


class Status(Enum):
    IDLE = 0
    PENDING = 1
    ACCEPTED = 2
    REJECTED = 3


# Replica id is a unique identifier that helps differentiate between different replicas
replica_id = socket.gethostname()


class RabbitMQHandler:
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_HOST))
        self.channel = self.connection.channel()

        self.channel.exchange_declare(DLX_EXCHANGE, "direct")

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
        start = time.time()
        while self.response is None:
            self.connection.process_data_events(time_limit=0.5)
            if time.time() - start > MESSAGE_TTL:
                raise TimeoutError("RPC call timed out")
        return self.response

    def close_connection(self):
        self.connection.close()


def get_log_info_from_db() -> LogInfo | None:
    try:
        # get serialized data
        entry: bytes = db.get(replica_id)
    except redis.exceptions.RedisError:
        return None
    # deserialize data if it exists else return null
    entry: OrderValue | None = msgpack.decode(entry, type=LogInfo) if entry else None
    return entry


def get_order_from_db_no_abort(order_id: str) -> OrderValue | None:
    try:
        entry: bytes = db.get(order_id)
    except redis.exceptions.RedisError:
        return None
    # deserialize data if it exists else return null
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    return entry


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


def rollback_stock_async(
    order_id: str, items: list[tuple[str, int]], log_info: LogInfo
):
    """Publish a rollback stock event to the Stock Service Queue."""
    # Send the log_id of the stock rollback operation
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in items:
        items_quantities[item_id] -= quantity

    stock_service_message = {
        "items": items_quantities,
        "log_id": log_info.stock_rollback_log,
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


def rollback_payment_async(order_id: str, order_entry: OrderValue, log_info: LogInfo):
    """Publish refund user event to the Payment Service Queue."""
    # Send the log_id of the payment rollback operation
    payment_service_message = {
        "user_id": order_entry.user_id,
        "total_cost": -order_entry.total_cost,
        "log_id": log_info.payment_rollback_log,
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
    app.logger.debug(f"Checking out {order_id}.")

    order_entry: OrderValue = get_order_from_db(order_id)

    # If this order was already checked out (either in progress or completed), we
    # should avoid checking it out again.
    if order_entry.order_status == Status.PENDING.value:
        app.logger.debug(
            f"The process of checking out order {order_id} has already started. This "
            + "request is aborted."
        )
        return Response(
            f"The process of checking out order {order_id} has already started. This "
            + "request is aborted.",
            status=400,
        )

    if order_entry.order_status != Status.IDLE.value:
        app.logger.debug(
            f"The process of checking out order {order_id} has FINISHED. This "
            + "request is aborted."
        )
        return Response(
            f"The process of checking out order {order_id} has FINISHED. This "
            + "request is aborted.",
            status=400,
        )

    # Generate log_info to retrieve the log IDs for all possible operations.
    # There are 4 possible operations: a transaction and a rollback for each service.
    # For this reason log_info has 4 fields.
    # It's important to generate these in advance and store them in the database,
    # rather than generating them on the fly, so that the service can check for
    # any incomplete transactions in the event of a system failure.

    log_info = generate_log_info(order_id)

    # Update the status of all three steps to PENDING, indicating that the checkout
    # procedure was initiated.

    order_entry.stock_status = Status.PENDING.value
    order_entry.order_status = Status.PENDING.value
    order_entry.payment_status = Status.PENDING.value
    try:
        # Here we initialize the logging process.
        # Once we update the status of the order we save to the database the log ids
        # of the possible operations. replica_id is utilized as a key to  differentiate
        # between processes of diffrent replicas. So that a newly restarted replica
        # cannot mistake other ongoing processes as though they have crashed
        initialize_checkout(
            keys=[order_id, replica_id],
            args=[msgpack.encode(order_entry), msgpack.encode(log_info)],
        )
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)

    # Crash before anything was done
    # os._exit(1)

    # get the quantity per item.
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity

    # Send subtract stock event to the Stock Service via RPC and wait until it replies.
    # Also send the log_id of the stock transaction
    stock_service_message = {
        "order_id": order_id,
        "log_id": log_info.stock_log,
        "items": items_quantities,
        "type": "action",
    }
    try:
        stock_service_response = rabbitmq_handler.call(
            STOCK_SERVICE_REQUESTS_QUEUE, stock_service_message
        )
    except TimeoutError:
        app.logger.error(f"Stock service RPC timed out for order {order_id}")

        order_entry.stock_status = Status.IDLE.value
        order_entry.order_status = Status.IDLE.value
        order_entry.payment_status = Status.IDLE.value
        try:
            # Once we decide on the outcome of the order
            # The entry of log_info in the database is deleted
            # Meaning that there is no longer an ongoing process in this replica
            finalize_checkout(
                keys=[order_id, replica_id], args=[msgpack.encode(order_entry)]
            )
        except redis.exceptions.RedisError:
            return abort(400, DB_ERROR_STR)

        return Response("Stock service did not respond in time.", status=504)

    response_status_from_stock_service = stock_service_response["status"]

    app.logger.debug(
        f"Stock Service replied to the subtract stock action for order {order_id}."
    )

    # Crash when only the stock has been updated
    # os._exit(1)

    # Send charge user event to the Payment Service via RPC and wait until it replies.
    # Also send the log_id of the payment transaction
    payment_service_message = {
        "user_id": order_entry.user_id,
        "total_cost": order_entry.total_cost,
        "log_id": log_info.payment_log,
        "order_id": order_id,
        "type": "action",
    }

    try:
        payment_service_response = rabbitmq_handler.call(
            PAYMENT_SERVICE_REQUESTS_QUEUE, payment_service_message
        )
    except TimeoutError:
        app.logger.error(f"Payment service RPC timed out for order {order_id}")

        if response_status_from_stock_service == 200:
            rollback_stock_async(order_id, order_entry.items, log_info)

        order_entry.stock_status = Status.IDLE.value
        order_entry.order_status = Status.IDLE.value
        order_entry.payment_status = Status.IDLE.value
        try:
            # Once we decide on the outcome of the order
            # The entry of log_info in the database is deleted
            # Meaning that there is no longer an ongoing process in this replica
            finalize_checkout(
                keys=[order_id, replica_id], args=[msgpack.encode(order_entry)]
            )
        except redis.exceptions.RedisError:
            return abort(400, DB_ERROR_STR)
        return Response("Payment service did not respond in time.", status=504)

    app.logger.debug(
        f"Payment Service replied to the charge user action for order {order_id}."
    )
    response_status_from_payment_service = payment_service_response["status"]
    # Crash when both the stock and payment have been updated but no rollback
    # os._exit(1)

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
            rollback_stock_async(order_id, order_entry.items, log_info)
        if response_status_from_payment_service == 200:
            # Payment was successful, but stock update has failed. Refund the user.
            rollback_payment_async(order_id, order_entry, log_info)

        # Crash after the rollbacks
        # os._exit(1)
    try:
        # Once we decide on the outcome of the order
        # The entry of log_info in the database is deleted
        # Meaning that there is no longer an ongoing process in this replica
        finalize_checkout(
            keys=[order_id, replica_id], args=[msgpack.encode(order_entry)]
        )
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)

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


def check_for_failed_processes():
    """
    Checks the database for any unfinished or failed processes (orders), and triggers
    compensation steps if needed.

    This function:
        1. Retrieves the most recent log entry from the database
            (it utilizes socketname as replica_id in order
            to check for its own failed processes).
        2. If an unfinished process is found:
           - Attempts to load the corresponding order data.
           - Publishes "compensation" messages to the stock and payment services
             via RabbitMQ for rollback.
           - Resets the order statuses to 'IDLE'.
           - Finalizes checkout by updating the order in the database.
        3. If no unfinished process is found, logs that nothing needs handling.

    Returns:
        None
    """
    app.logger.info(f"Running custom startup code under Gunicorn, replica:{replica_id}")
    log_info = get_log_info_from_db()
    if log_info:
        app.logger.info(
            f"LogInfo - order_id: {log_info.order_id}, "
            f"stock_log: {log_info.stock_log}, "
            f"stock_rollback_log: {log_info.stock_rollback_log}, "
            f"payment_log: {log_info.payment_log}, "
            f"payment_rollback_log: {log_info.payment_rollback_log}"
        )
        order_entry: OrderValue = get_order_from_db_no_abort(log_info.order_id)

        if not order_entry:
            return

        items_quantities: dict[str, int] = defaultdict(int)

        for item_id, quantity in order_entry.items:
            items_quantities[item_id] -= quantity

        # target entry log is utilized so that stock does not rollback
        # an unexisting update to the stock/payment.

        stock_service_message = {
            "items": items_quantities,
            "log_id": log_info.stock_rollback_log,
            "target_entry_log": log_info.stock_log,
            "order_id": log_info.order_id,
            "type": "compensation",
        }

        rabbitmq_handler.channel.basic_publish(
            exchange=DLX_EXCHANGE,
            routing_key=STOCK_DLX_KEY,
            body=json.dumps(stock_service_message),
        )

        payment_service_message = {
            "user_id": order_entry.user_id,
            "total_cost": -order_entry.total_cost,
            "log_id": log_info.payment_rollback_log,
            "target_entry_log": log_info.payment_log,
            "order_id": log_info.order_id,
            "type": "compensation",
        }

        rabbitmq_handler.channel.basic_publish(
            exchange=DLX_EXCHANGE,
            routing_key=PAYMENT_DLX_KEY,
            body=json.dumps(payment_service_message),
        )

        order_entry.stock_status = Status.IDLE.value
        order_entry.order_status = Status.IDLE.value
        order_entry.payment_status = Status.IDLE.value

        finalize_checkout(
            keys=[log_info.order_id, replica_id], args=[msgpack.encode(order_entry)]
        )
    else:
        app.logger.info("There is no unfinished process")


if __name__ == "__main__":
    http_server = WSGIServer(("0.0.0.0", 5000), app)
    http_server.spawn = 4
    http_server.serve_forever()
    # app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
    check_for_failed_processes()
