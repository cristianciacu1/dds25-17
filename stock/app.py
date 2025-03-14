import logging
import os
import atexit
import uuid

import pika
import threading
import redis
import json

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


check_and_update_stock_script = """
    -- Phase 1: Check if there's enough stock for all items
    for i=1, #KEYS do
        local item_id = tostring(KEYS[i])
        local quantity = tonumber(ARGV[i])

        -- Get current stock for the item
        local current_stock = tonumber(redis.pcall('HGET', item_id, 'stock'))
        if not current_stock then
            return {false, "item_not_found", item_id}
        elseif type(current_stock) == "table" and current_stock.err then
            return {false, "database_error_during_check", item_id, current_stock.err}
        else
            current_stock = tonumber(current_stock)
        end

        -- Check if there's enough stock
        if current_stock < quantity then
            return {false, "not_enough_stock", item_id, current_stock, quantity}
        end
    end

    -- Phase 2: If all checks passed, update stock for all items
    for i=1, #KEYS do
        local item_id = tostring(KEYS[i])
        local quantity = tonumber(ARGV[i])

        -- Update stock atomically
        local reply = redis.pcall('HINCRBY', item_id, 'stock', -quantity)
        if type(reply) == "table" and reply.err then
            return {false, "database_error_during_update", item_id, reply.err}
        end
    end

    return {true, "success"}
"""

check_and_update_stock = db.register_script(check_and_update_stock_script)


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

        keys = list(order_items_quantities.keys())
        args = list(order_items_quantities.values()) + [order_id]

        try:
            result = check_and_update_stock(keys, args)
        except redis.exceptions.ConnectionError as e:
            response_message = f"For order {order_id}, there were issues with the connection to the database."
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
            # In case there was enough stock for the entire order, then publish SUCCESS
            # message to the Order Checkout saga replies queue.
            if order_type == "action":
                response_message = (
                    f"For order {order_id}, stock was successfully updated."
                )
                self.publish_message(
                    method,
                    properties,
                    response_message,
                    200,
                    order_id,
                )
            else:
                # If a rollback was performed, then log the outcome.
                # No need to publish message, since the Order Service does not expect
                # a response from the compensation function.
                app.logger.debug(
                    f"For order {order_id}, the stock was rolled back successfully."
                )
        else:
            # Handle different error cases.
            error_type = result[1]
            error_item = result[2]
            match error_type:
                case "item_not_found":
                    response_message = f"For order {order_id}, item {error_item} was "
                    +"not found in the database."
                case "database_error_during_check" | "database_error_during_update":
                    error = result[3]
                    response_message = f"Order {order_id} could not be processed since there was an error with the database.\n{error}"
                case "not_enough_stock":
                    current_stock = result[3]
                    requested_quantity = result[4]
                    response_message = (
                        f"For order {order_id}, there was not enough stock for item "
                        + f"{error_item}. Available: {current_stock}, Requested: "
                        + f"{requested_quantity}."
                    )
                case _:
                    response_message = (
                        f"For order {order_id}, an unknown error occurred: {error_type}"
                    )

            # Publish message.
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
            "type": "stock",
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
            queue=STOCK_SERVICE_REQUESTS_QUEUE, on_message_callback=self.callback
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


def get_item_from_db(item_id: str) -> dict | None:
    entry = decode_redis(db.hgetall(item_id))
    app.logger.debug(entry)
    if not entry:
        abort(400, f"Item {item_id} not found.")
    return entry


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
    try:
        db.hset(key, mapping={"stock": 0, "price": int(price)})
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"item_id": key})


@app.post("/batch_init/<n>/<starting_stock>/<item_price>")
def batch_init_users(n: int, starting_stock: int, item_price: int):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    # TODO: Very slow. Consider creating a Lua script for adding items in batches.
    for i in range(n):
        db.hset(str(i), mapping={"stock": starting_stock, "price": item_price})
    return jsonify({"msg": "Batch init for stock successful"})


@app.get("/find/<item_id>")
def find_item(item_id: str):
    app.logger.debug(f"Item {item_id} is searched.")
    item_entry = get_item_from_db(item_id)
    return jsonify(item_entry)


@app.post("/add/<item_id>/<amount>")
def add_stock(item_id: str, amount: int):
    item_entry = get_item_from_db(item_id)
    # update stock, serialize and update database
    new_stock = int(item_entry["stock"]) + int(amount)
    try:
        db.hset(item_id, mapping={"stock": new_stock, "price": item_entry["price"]})
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(
        f"Item: {item_id} stock updated to: {item_entry['stock']}", status=200
    )


@app.post("/subtract/<item_id>/<amount>")
def remove_stock(item_id: str, amount: int):
    item_entry = get_item_from_db(item_id)
    # update stock, serialize and update database
    new_value = item_entry["stock"] - int(amount)
    app.logger.debug(f"Item: {item_id} stock updated to: {new_value}")
    if new_value < 0:
        abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
    try:
        db.hset(item_id, mapping={"stock": new_value, "price": item_entry["price"]})
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(
        f"Item: {item_id} stock updated to: {item_entry['stock']}", status=200
    )


atexit.register(close_db_connection)
# atexit.register(rabbitmq_handler.close_connection)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
