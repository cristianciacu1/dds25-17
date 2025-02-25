import logging
import os
import atexit
import uuid
from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response
import pika


DB_ERROR_STR = "DB error"

app = Flask("stock-api")

class StockValue(Struct):
    stock: int
    price: int

class RabbitMQClient:
    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.URLParameters(os.environ["RABBITMQ_URL"])
        )
        self.channel = self.connection.channel()

        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)
        
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

@app.post("/item/create/<price>")
def create_item(price: int):
    response = rabbitMQ_client.call("stock_queue", {"function": "create_item", "price": price})
    return response


@app.post("/batch_init/<n>/<starting_stock>/<item_price>")
def batch_init_users(n: int, starting_stock: int, item_price: int):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    kv_pairs: dict[str, bytes] = {
        f"{i}": msgpack.encode(StockValue(stock=starting_stock, price=item_price))
        for i in range(n)
    }
    response = rabbitMQ_client.call("stock_queue", {"function": "batch_init_users", "kv_pairs": kv_pairs})
    return response


@app.get("/find/<item_id>")
def find_item(item_id: str):
    response = rabbitMQ_client.call("stock_queue", {"function": "find_item", "item_id": item_id})
    return response["entry"]


@app.post("/add/<item_id>/<amount>")
def add_stock(item_id: str, amount: int):
    response = rabbitMQ_client.call("stock_queue", {"function": "add_stock", "item_id": item_id, "amount": amount})
    return response


@app.post("/subtract/<item_id>/<amount>")
def remove_stock(item_id: str, amount: int):
    response = rabbitMQ_client.call("stock_queue", {"function": "remove_stock", "item_id": item_id, "amount": amount})
    return response


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
