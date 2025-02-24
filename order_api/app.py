import json
import logging
import os
import random
import uuid
from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response
import pika

app = Flask("order-api")


class RabbitMQClient:
    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.URLParameters(os.environ["RABBITMQ_URL"])
        )
        self.channel = self.connection.channel()

        # Declare a callback queue for responses
        result = self.channel.queue_declare(queue="", exclusive=True)
        self.callback_queue = result.method.queue

        # Set up consumer for the callback queue
        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True,
        )

        self.responses = {}

    def on_response(self, ch, method, properties, body):
        """Handle response messages."""
        self.responses[properties.correlation_id] = msgpack.decode(body)

    def call(self, request_body):
        """Send message to queue and wait for response."""
        corr_id = str(uuid.uuid4())  # Generate unique ID
        self.responses[corr_id] = None  # Initialize storage for response

        self.channel.basic_publish(
            exchange="",
            routing_key="order_queue",
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=corr_id,
            ),
            body=msgpack.encode(request_body),
        )

        # Wait for response
        while self.responses[corr_id] is None:
            self.connection.process_data_events()

        return self.responses.pop(corr_id)  # Return the response


rabbitMQ_client = RabbitMQClient()


class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


@app.post("/create/<user_id>")
def create_order(user_id: str):
    response = rabbitMQ_client.call({"function": "create_order", "user_id": user_id})
    return response


# @app.post("/batch_init/<n>/<n_items>/<n_users>/<item_price>")
# def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):

#     n = int(n)
#     n_items = int(n_items)
#     n_users = int(n_users)
#     item_price = int(item_price)

#     def generate_entry() -> OrderValue:
#         user_id = random.randint(0, n_users - 1)
#         item1_id = random.randint(0, n_items - 1)
#         item2_id = random.randint(0, n_items - 1)
#         value = OrderValue(
#             paid=False,
#             items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
#             user_id=f"{user_id}",
#             total_cost=2 * item_price,
#         )
#         return value

#     kv_pairs: dict[str, bytes] = {
#         f"{i}": msgpack.encode(generate_entry()) for i in range(n)
#     }
#     try:
#         db.mset(kv_pairs)
#     except redis.exceptions.RedisError:
#         return abort(400, DB_ERROR_STR)
#     return jsonify({"msg": "Batch init for orders successful"})


# @app.get("/find/<order_id>")
# def find_order(order_id: str):
#     order_entry: OrderValue = get_order_from_db(order_id)
#     return jsonify(
#         {
#             "order_id": order_id,
#             "paid": order_entry.paid,
#             "items": order_entry.items,
#             "user_id": order_entry.user_id,
#             "total_cost": order_entry.total_cost,
#         }
#     )


# def send_post_request(url: str):
#     try:
#         response = requests.post(url)
#     except requests.exceptions.RequestException:
#         abort(400, REQ_ERROR_STR)
#     else:
#         return response


# def send_get_request(url: str):
#     try:
#         response = requests.get(url)
#     except requests.exceptions.RequestException:
#         abort(400, REQ_ERROR_STR)
#     else:
#         return response


# @app.post("/addItem/<order_id>/<item_id>/<quantity>")
# def add_item(order_id: str, item_id: str, quantity: int):
#     order_entry: OrderValue = get_order_from_db(order_id)
#     item_reply = send_get_request(f"{GATEWAY_URL}/stock/find/{item_id}")
#     if item_reply.status_code != 200:
#         # Request failed because item does not exist
#         abort(400, f"Item: {item_id} does not exist!")
#     item_json: dict = item_reply.json()
#     order_entry.items.append((item_id, int(quantity)))
#     order_entry.total_cost += int(quantity) * item_json["price"]
#     try:
#         db.set(order_id, msgpack.encode(order_entry))
#     except redis.exceptions.RedisError:
#         return abort(400, DB_ERROR_STR)
#     return Response(
#         f"Item: {item_id} added to: {order_id} "
#         f"price updated to: {order_entry.total_cost}",
#         status=200,
#     )


# def rollback_stock(removed_items: list[tuple[str, int]]):
#     for item_id, quantity in removed_items:
#         send_post_request(f"{GATEWAY_URL}/stock/add/{item_id}/{quantity}")


# @app.post("/checkout/<order_id>")
# def checkout(order_id: str):
#     app.logger.debug(f"Checking out {order_id}")
#     order_entry: OrderValue = get_order_from_db(order_id)
#     # get the quantity per item
#     items_quantities: dict[str, int] = defaultdict(int)
#     for item_id, quantity in order_entry.items:
#         items_quantities[item_id] += quantity
#     # The removed items will contain the items that we already have successfully
#     # subtracted stock from for rollback purposes.
#     removed_items: list[tuple[str, int]] = []
#     for item_id, quantity in items_quantities.items():
#         stock_reply = send_post_request(
#             f"{GATEWAY_URL}/stock/subtract/{item_id}/{quantity}"
#         )
#         if stock_reply.status_code != 200:
#             # If one item does not have enough stock we need to rollback
#             rollback_stock(removed_items)
#             abort(400, f"Out of stock on item_id: {item_id}")
#         removed_items.append((item_id, quantity))
#     user_reply = send_post_request(
#         f"{GATEWAY_URL}/payment/pay/{order_entry.user_id}/{order_entry.total_cost}"
#     )
#     if user_reply.status_code != 200:
#         # If the user does not have enough credit we need to rollback all the item
#         # stock subtractions.
#         rollback_stock(removed_items)
#         abort(400, "User out of credit")
#     order_entry.paid = True
#     try:
#         db.set(order_id, msgpack.encode(order_entry))
#     except redis.exceptions.RedisError:
#         return abort(400, DB_ERROR_STR)
#     app.logger.debug("Checkout successful")
#     return Response("Checkout successful", status=200)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
