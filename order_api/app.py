from collections import defaultdict
import logging
import os
import random
import uuid
from msgspec import msgpack, Struct
from flask import Flask, abort
import pika

app = Flask("order-api")


class RabbitMQClient:
    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.URLParameters(os.environ["RABBITMQ_URL"])
        )
        self.channel = self.connection.channel()

        result = self.channel.queue_declare(queue="", exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True,
        )

        self.response = None
        self.corr_id = None

    def on_response(self, ch, method, properties, body):
        """Handle response messages."""
        if self.corr_id == properties.correlation_id:
            self.response = msgpack.decode(body)

    def call(self, queue_name, request_body):
        """Send a message to the specified queue and wait for a response."""
        self.corr_id = str(uuid.uuid4())  # Generate unique ID
        self.response = None  # Initialize storage for response

        self.channel.basic_publish(
            exchange="",
            routing_key=queue_name,
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,  # Use specific reply queue
                correlation_id=self.corr_id,
            ),
            body=msgpack.encode(request_body),
        )

        # Wait for response
        while self.response is None:
            self.connection.process_data_events()

        return self.response  # Return the response


rabbitMQ_client = RabbitMQClient()


class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


@app.post("/create/<user_id>")
def create_order(user_id: str):
    response = rabbitMQ_client.call(
        "order_queue", {"function": "create_order", "user_id": user_id}
    )
    return response


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
            paid=False,
            items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
            user_id=f"{user_id}",
            total_cost=2 * item_price,
        )
        return value

    kv_pairs: dict[str, bytes] = {
        f"{i}": msgpack.encode(generate_entry()) for i in range(n)
    }
    response = rabbitMQ_client.call(
        "order_queue", {"function": "batch_init_users", "kv_pairs": kv_pairs}
    )
    return response


@app.get("/find/<order_id>")
def find_order(order_id: str):
    response = rabbitMQ_client.call(
        "order_queue", {"function": "find_order", "order_id": order_id}
    )
    return response


@app.post("/addItem/<order_id>/<item_id>/<quantity>")
def add_item(order_id: str, item_id: str, quantity: int):
    order_entry_response = rabbitMQ_client.call(
        "order_queue", {"function": "find_order", "order_id": order_id}
    )
    order_entry = order_entry_response

    item_reply_response = rabbitMQ_client.call(
        "stock_queue", {"function": "find_item", "item_id": item_id}
    )
    item_reply = item_reply_response["entry"]
    if item_reply_response["status"] != 200:
        abort(400, f"Item: {item_id} does not exist!")

    order_entry["items"].append((item_id, int(quantity)))
    order_entry["total_cost"] += int(quantity) * item_reply["price"]

    response = rabbitMQ_client.call(
        "order_queue",
        {"function": "add_item", "order_entry": order_entry, "order_id": order_id},
    )
    return response


def rollback_stock(removed_items: list[tuple[str, int]]):
    for item_id, quantity in removed_items:
        rabbitMQ_client.call(
            "stock_queue",
            {"function": "add_stock", "item_id": item_id, "amount": quantity},
        )


@app.post("/checkout/<order_id>")
def checkout(order_id: str):
    app.logger.debug(f"Checking out {order_id}")
    order_entry_response = rabbitMQ_client.call(
        "order_queue", {"function": "find_order", "order_id": order_id}
    )
    order_entry = order_entry_response
    # get the quantity per item
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry["items"]:
        items_quantities[item_id] += quantity
    # The removed items will contain the items that we already have successfully
    # subtracted stock from for rollback purposes.
    removed_items: list[tuple[str, int]] = []
    for item_id, quantity in items_quantities.items():
        stock_subtract_response = rabbitMQ_client.call(
            "stock_queue",
            {"function": "remove_stock", "item_id": item_id, "amount": quantity},
        )
        if stock_subtract_response["status"] != 200:
            # If one item does not have enough stock we need to rollback
            rollback_stock(removed_items)
            abort(400, f"Out of stock on item_id: {item_id}")
        removed_items.append((item_id, quantity))
    user_pay_response = rabbitMQ_client.call(
        "payment_queue",
        {
            "function": "remove_credit",
            "user_id": order_entry["user_id"],
            "amount": order_entry["total_cost"],
        },
    )
    if user_pay_response["status"] != 200:
        # If the user does not have enough credit we need to rollback all the item
        # stock subtractions.
        rollback_stock(removed_items)
        abort(400, "User out of credit")
    order_entry["paid"] = True

    response = rabbitMQ_client.call(
        "order_queue",
        {"function": "checkout", "order_entry": order_entry, "order_id": order_id},
    )
    return response


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
