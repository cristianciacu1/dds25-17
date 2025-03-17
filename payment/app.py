import atexit
import json
import logging
import os
import pika
import redis
import threading
import uuid

from flask import Flask, jsonify, abort, Response

DB_ERROR_STR = "DB error"
PAYMENT_SERVICE_REQUESTS_QUEUE = os.environ["PAYMENT_SERVICE_REQUESTS_QUEUE"]
ORDER_CHECKOUT_SAGA_REPLIES_QUEUE = os.environ["ORDER_CHECKOUT_SAGA_REPLIES_QUEUE"]
RABBITMQ_HOST = os.environ["RABBITMQ_URL"]

app = Flask("payment-service")


db: redis.Redis = redis.Redis(
    host=os.environ["REDIS_HOST"],
    port=int(os.environ["REDIS_PORT"]),
    password=os.environ["REDIS_PASSWORD"],
    db=int(os.environ["REDIS_DB"]),
)


def close_db_connection():
    db.close()


atexit.register(close_db_connection)

check_and_charge_user_script = """
    local user_id = KEYS[1]
    local total_cost = tonumber(ARGV[1])

    local available_funds = redis.pcall('HGET', user_id, 'credit')
    if not available_funds then
        return {false, "user_not_found", user_id}
    elseif type(available_funds) == "table" and available_funds.err then
        return {false, "database_error_during_check", user_id, available_funds.err}
    else
        available_funds = tonumber(available_funds)
    end

    if available_funds < total_cost then
        return {false, "not_enough_funds", user_id}
    end

    local reply = redis.pcall('HINCRBY', user_id, 'credit', -total_cost)
    if type(reply) == "table" and reply.err then
        return {false, "database_error_during_update", user_id, reply.err}
    end

    return {true, "success"}
"""

check_and_charge_user = db.register_script(check_and_charge_user_script)


class RabbitMQHandler:
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_HOST))
        self.channel = self.connection.channel()

        # Declare queues
        self.channel.queue_declare(queue=ORDER_CHECKOUT_SAGA_REPLIES_QUEUE)
        self.channel.queue_declare(queue=PAYMENT_SERVICE_REQUESTS_QUEUE)

    def callback(self, ch, method, properties, body):
        """Callback function to process messages from RabbitMQ queue."""
        # Expected message type: {'user_id': int, 'total_cost': int}.
        message = json.loads(body.decode())
        user_id = message["user_id"]
        amount = message["total_cost"]
        order_id = message["order_id"]
        order_type = message["type"]

        try:
            result = check_and_charge_user([user_id], [amount])
        except redis.exceptions.ConnectionError as e:
            response_message = (
                f"When trying to charge/refund user {user_id}, there were issues "
                + f"with the connection to the database.\n{e}"
            )
            self.publish_message(
                method,
                properties,
                response_message,
                400,
                order_id,
            )
            return

        status_of_result = result[0]

        if status_of_result:
            # If successful and a saga action was performed then publish SUCCESS
            # event to the order checkout saga replies queue.
            if order_type == "action":
                self.publish_message(
                    method,
                    properties,
                    (
                        f"For order {order_id}, the user {user_id} was "
                        + "charged successfully."
                    ),
                    200,
                    order_id,
                )
            else:
                # If a saga compensation action was performed,
                # then just log the outcome.
                app.logger.debug(
                    f"For order {order_id}, the user {user_id} was refunded "
                    + "successfully."
                )
        else:
            # Handle different error cases.
            error_type = result[1].decode("utf-8")

            match error_type:
                case "user_not_found":
                    response_message = (
                        f"For order {order_id}, the user {user_id} does not exist."
                    )
                case "not_enough_funds":
                    response_message = (
                        f"For order {order_id}, the user {user_id} does not have "
                        + "enough funds."
                    )
                case "database_error_during_check" | "database_error_during_update":
                    error = result[3].decode("utf-8")
                    response_message = (
                        f"User {user_id} could not be charged since there was an "
                        + f"error with the database.\n{error}"
                    )
                case _:
                    response_message = (
                        f"When charging user {user_id} for order {order_id}, an "
                        + f"unknown error occurred: {error_type}"
                    )

            # Publish message.
            if order_type == 'action':
                self.publish_message(
                    method,
                    properties,
                    response_message,
                    400,
                    order_id,
                )

    def publish_message(
        self,
        method,
        props,
        message,
        status_code,
        order_id,
        queue=None,
        is_compensation=False,
    ):
        response = {
            "message": message,
            "status": status_code,
            "order_id": order_id,
            "type": "payment",
        }
        if not is_compensation:
            self.channel.basic_publish(
                exchange="",
                routing_key=props.reply_to,
                properties=pika.BasicProperties(correlation_id=props.correlation_id),
                body=json.dumps(response),
            )
            self.channel.basic_ack(delivery_tag=method.delivery_tag)
        else:
            self.channel.basic_publish(
                exchange="",
                routing_key=queue,
                body=json.dumps(response),
            )
        app.logger.debug(message)

    def start_consuming(self):
        # self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue=PAYMENT_SERVICE_REQUESTS_QUEUE,
            on_message_callback=self.callback,
        )
        self.channel.start_consuming()
        app.logger.debug("Started listening to stock service requests...")

    def close_connection(self):
        self.connection.close()


def decode_redis(src):
    """Decode response from Redis. Note that this implementation is suitable only
    for values (value from the key-value pair) that are supposed to be integers.
    """
    if isinstance(src, list):
        rv = list()
        for key in src:
            rv.append(decode_redis(key))
        return rv
    elif isinstance(src, dict):
        rv = dict()
        for key in src:
            rv[key.decode()] = decode_redis(src[key])
        return rv
    elif isinstance(src, bytes):
        return int(src.decode())
    else:
        raise Exception("type not handled: " + type(src))


def get_user_from_db(user_id: str) -> dict | None:
    entry = decode_redis(db.hgetall(user_id))
    if not entry:
        # if user does not exist in the database; abort
        abort(400, f"User: {user_id} not found!")
    return entry


# Create a single instance of RabbitMQHandler
rabbitmq_handler = RabbitMQHandler()


# Run consumer in a separate thread
def start_consumer():
    rabbitmq_handler.start_consuming()


# Start RabbitMQ Consumer in a separate thread.
consumer_thread = threading.Thread(target=start_consumer, daemon=True)
consumer_thread.start()


@app.post("/create_user")
def create_user():
    key = str(uuid.uuid4())
    db.hset(key, "credit", 0)
    return jsonify({"user_id": key})


@app.post("/batch_init/<n>/<starting_money>")
def batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    # TODO: Very slow. Consider creating a Lua script for adding users in batches.
    for i in range(n):
        db.hset(str(i), "credit", starting_money)
    return jsonify({"msg": "Batch init for users successful"})


@app.get("/find_user/<user_id>")
def find_user(user_id: str):
    user_entry: dict = get_user_from_db(user_id)
    return jsonify(user_entry)


@app.post("/add_funds/<user_id>/<amount>")
def add_credit(user_id: str, amount: int):
    user_entry: dict = get_user_from_db(user_id)
    # update credit, serialize and update database
    new_credit = user_entry["credit"] + int(amount)
    try:
        db.hset(user_id, "credit", new_credit)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {new_credit}", status=200)


@app.post("/pay/<user_id>/<amount>")
def remove_credit(user_id: str, amount: int):
    app.logger.debug(f"Removing {amount} credit from user: {user_id}")
    user_entry: dict = get_user_from_db(user_id)
    # update credit, serialize and update database
    new_credit = user_entry["credit"] - int(amount)
    if user_entry["credit"] < 0:
        abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    try:
        db.hset(user_id, "credit", new_credit)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(
        f"User: {user_id} credit updated to: {user_entry['credit']}", status=200
    )


atexit.register(close_db_connection)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
