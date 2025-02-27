from enum import Enum
import logging
import os
import atexit
import random
import uuid
import threading
from collections import defaultdict

import json
import pika
import redis
import requests

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

class Status(Enum):
    IDLE = 0
    PENDING = 1
    ACCEPTED = 2
    REJECTED = 3

atexit.register(close_db_connection)


class OrderValue(Struct):
    stock_status: int
    payment_status: int
    order_status: int
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


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


@app.post("/create/<user_id>")
def create_order(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(
        OrderValue(stock_status = Status.IDLE.value, payment_status = Status.IDLE.value, order_status = Status.IDLE.value, items=[], user_id=user_id, total_cost=0)
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
            stock_status = Status.IDLE.value, payment_status = Status.IDLE.value, order_status = Status.IDLE.value,
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
            "stock_status": Status(order_entry.stock_status),
            "payment_status": Status(order_entry.payment_status),
            "order_status": Status(order_entry.order_status),
            "items": order_entry.items,
            "user_id": order_entry.user_id,
            "total_cost": order_entry.total_cost,
        }
    )


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
        status=200
    )


def rollback_stock(removed_items: list[tuple[str, int]]):
    for item_id, quantity in removed_items:
        send_post_request(f"{GATEWAY_URL}/stock/add/{item_id}/{quantity}")


def rollback_stock_async(order_id: str, items: list[tuple[str, int]]):
    connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_HOST))
    channel = connection.channel()
    
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in items:
        items_quantities[item_id] -= quantity

    stock_service_message = {
        "items_quantities": items_quantities,
        "order_id": order_id,
        "type": "compensation"
    }

    # Publish subtract stock event to the Stock Service Queue.
    channel.queue_declare(queue=STOCK_SERVICE_REQUESTS_QUEUE)
    channel.basic_publish(
        exchange="",
        routing_key=STOCK_SERVICE_REQUESTS_QUEUE,
        body=json.dumps(stock_service_message),
    )
    app.logger.info(f"For order {order_id}, rollback stock action pushed to the Stock Service.")

    connection.close()


def rollback_payment_async(order_id: str, order_entry: OrderValue):
    connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_HOST))
    channel = connection.channel()

    # Publish refund user event to the Payment Service Queue.
    channel.queue_declare(queue=PAYMENT_SERVICE_REQUESTS_QUEUE)
    payment_service_message = {
        "user_id": order_entry.user_id,
        "total_cost": -order_entry.total_cost,
        "order_id": order_id,
        "type": "compensation"
    }
    channel.basic_publish(
        exchange="",
        routing_key=PAYMENT_SERVICE_REQUESTS_QUEUE,
        body=json.dumps(payment_service_message),
    )
    app.logger.info(f"For order {order_id}, refund user action pushed to the Payment Service.")

    connection.close()

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
    order_entry.order_status = Status.ACCEPTED.value
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    app.logger.debug("Checkout successful")
    return Response("Checkout successful", status=200)


@app.post("/checkout/<order_id>")
async def checkout(order_id: str):
    connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_HOST))
    channel = connection.channel()
    
    app.logger.info(f"Checking out {order_id}.")
    order_entry: OrderValue = get_order_from_db(order_id)
    if order_entry.order_status != Status.IDLE.value:
        return Response(
            f"The process of checking out order {order_id} has already started.",
            status=200,
        )
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

    stock_service_message = {
        "items_quantities": items_quantities,
        "order_id": order_id,
        "type": "action"
    }

    # Publish subtract stock event to the Stock Service Queue.
    channel.queue_declare(queue=STOCK_SERVICE_REQUESTS_QUEUE)
    channel.basic_publish(
        exchange="",
        routing_key=STOCK_SERVICE_REQUESTS_QUEUE,
        body=json.dumps(stock_service_message),
    )

    # Publish charge user event to the Payment Service Queue.
    channel.queue_declare(queue=PAYMENT_SERVICE_REQUESTS_QUEUE)
    payment_service_message = {
        "user_id": order_entry.user_id,
        "total_cost": order_entry.total_cost,
        "order_id": order_id,
        "type": "action"
    }
    channel.basic_publish(
        exchange="",
        routing_key=PAYMENT_SERVICE_REQUESTS_QUEUE,
        body=json.dumps(payment_service_message),
    )

    connection.close()
    return Response(
        f"The process of checking out order {order_id} has started. "
        + "Please check later the status of your order.",
        status=200,
    )


def process_received_message(ch, method, properties, body):
    """Callback function to process messages from RabbitMQ queue."""
    message = json.loads(body.decode())
    order_id = message["order_id"]
    order_entry: OrderValue = get_order_from_db(order_id)
    match message["type"]:
        case "payment":
            if message["status"] == 200:
                order_entry.payment_status = Status.ACCEPTED.value
                if order_entry.stock_status == Status.ACCEPTED.value:
                    order_entry.order_status = Status.ACCEPTED.value
                    app.logger.info(f"Order {order_id} was checked out successfully.")
                elif order_entry.stock_status == Status.REJECTED.value:
                    rollback_payment_async(order_id, order_entry)
                    order_entry.order_status = Status.REJECTED.value
                    order_entry.payment_status = Status.REJECTED.value
                    app.logger.info(f"Order {order_id} was rejected since payment was accepted but stock was not.")
            else: 
                order_entry.payment_status = Status.REJECTED.value
                order_entry.order_status = Status.REJECTED.value

                if order_entry.stock_status == Status.ACCEPTED.value:    
                    rollback_stock_async(order_id, order_entry.items)
                    order_entry.stock_status = Status.REJECTED.value
                    app.logger.info(f"Order {order_id} was rejected since stock was accepted but payment was not.")
                elif order_entry.stock_status == Status.REJECTED.value:
                    app.logger.info(f"Order {order_id} was rejected since both payment and stock were rejected.")
            try:
                db.set(order_id, msgpack.encode(order_entry))
            except redis.exceptions.RedisError:
                return abort(400, DB_ERROR_STR)
            return
        case "stock":
            if message["status"] == 200:
                order_entry.stock_status = Status.ACCEPTED.value
                if order_entry.payment_status == Status.ACCEPTED.value:
                    order_entry.order_status = Status.ACCEPTED.value
                    app.logger.info(f"Order {order_id} was checked out successfully.")
                elif order_entry.payment_status == Status.REJECTED.value:
                    rollback_stock_async(order_id, order_entry.items)
                    order_entry.order_status = Status.REJECTED.value
                    order_entry.stock_status = Status.REJECTED.value
                    app.logger.info(f"Order {order_id} was rejected since stock was accepted but payment was not.")
            else: 
                order_entry.stock_status = Status.REJECTED.value
                order_entry.order_status = Status.REJECTED.value

                if order_entry.payment_status == Status.ACCEPTED.value:
                    rollback_payment_async(order_id, order_entry)
                    order_entry.payment_status = Status.REJECTED.value
                    app.logger.info(f"Order {order_id} was rejected since payment was accepted but stock was not.")
                elif order_entry.payment_status == Status.REJECTED.value:
                    app.logger.info(f"Order {order_id} was rejected since both payment and stock were rejected.")
            try:
                db.set(order_id, msgpack.encode(order_entry))
            except redis.exceptions.RedisError:
                return abort(400, DB_ERROR_STR)
            return
        case _ :
            app.logger.info(f"Received unexpected message type: {message["type"]}.")
            return
    return


def consume_order_checkout_saga_replies_queue():
    """Continuously listen for messages on the order events queue."""

    # The following two lines need to be kept here. (otherwise, system crashes)
    connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_HOST))
    channel = connection.channel()

    # Ensure the queue exists
    channel.queue_declare(queue=ORDER_CHECKOUT_SAGA_REPLIES_QUEUE)

    # Start consuming messages
    channel.basic_consume(
        queue=ORDER_CHECKOUT_SAGA_REPLIES_QUEUE,
        on_message_callback=process_received_message,
        auto_ack=True
    )

    app.logger.info("Started listening to order checkout saga replies...")
    channel.start_consuming()


# Start RabbitMQ Consumer in a separate thread.
# Used for processing messages from checkout order saga replies.
consumer_thread = threading.Thread(
    target=consume_order_checkout_saga_replies_queue, daemon=True
)
consumer_thread.start()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
