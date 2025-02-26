import logging
import os
import uuid
from msgspec import msgpack, Struct
from flask import Flask
import pika

app = Flask("payment-api")


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


class UserValue(Struct):
    credit: int


@app.post("/create_user")
def create_user():
    response = rabbitMQ_client.call("payment_queue", {"function": "create_user"})
    return response


@app.post("/batch_init/<n>/<starting_money>")
def batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    kv_pairs: dict[str, bytes] = {
        f"{i}": msgpack.encode(UserValue(credit=starting_money)) for i in range(n)
    }
    response = rabbitMQ_client.call(
        "payment_queue", {"function": "batch_init_users", "kv_pairs": kv_pairs}
    )
    return response


@app.get("/find_user/<user_id>")
def find_user(user_id: str):
    response = rabbitMQ_client.call(
        "payment_queue", {"function": "find_user", "user_id": user_id}
    )
    return response


@app.post("/add_funds/<user_id>/<amount>")
def add_credit(user_id: str, amount: int):
    response = rabbitMQ_client.call(
        "payment_queue",
        {"function": "add_credit", "user_id": user_id, "amount": amount},
    )
    return response


@app.post("/pay/<user_id>/<amount>")
def remove_credit(user_id: str, amount: int):
    app.logger.debug(f"Removing {amount} credit from user: {user_id}")
    response = rabbitMQ_client.call(
        "payment_queue",
        {"function": "remove_credit", "user_id": user_id, "amount": amount},
    )
    return response


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
