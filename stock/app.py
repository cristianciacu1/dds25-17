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
MESSAGE_TTL = int(os.environ["MESSAGE_TTL"])
DLX_EXCHANGE = os.environ["DLX_EXCHANGE"]
DEAD_LETTER_STOCK_QUEUE = os.environ["DEAD_LETTER_STOCK_QUEUE"]
STOCK_DLX_KEY = os.environ["STOCK_DLX_KEY"]
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


revert_stock_update_script = """
    local log_id_to_find = ARGV[1]
    local stream_key = "stock_update_log"

    local entries = redis.pcall('XRANGE', stream_key, '-', '+')
    if type(entries) == "table" and entries.err then
        return {false, "stream_read_error", entries.err}
    end

    for _, entry in ipairs(entries) do
        local entry_id = entry[1]
        local fields = entry[2]

        local found_log_id = nil
        local items = nil
        local quantities = nil

        for i = 1, #fields, 2 do
            local field_name = fields[i]
            local field_value = fields[i+1]

            if field_name == "log_id" then
                found_log_id = field_value
            elseif field_name == "items" then
                items = field_value
            elseif field_name == "quantities" then
                quantities = field_value
            end
        end

        -- Step 2: If we find the matching log_id, revert stock
        if found_log_id == log_id_to_find then
            local item_ids = {}
            for id in string.gmatch(items, "([^,]+)") do
                table.insert(item_ids, id)
            end

            local qty_list = {}
            for qty in string.gmatch(quantities, "([^,]+)") do
                table.insert(qty_list, tonumber(qty))
            end

            if #item_ids ~= #qty_list then
                return {false, "log_data_malformed", log_id_to_find}
            end

            -- Step 3: Revert the stock updates
            for i = 1, #item_ids do
                local incr_reply = redis.pcall('HINCRBY', item_ids[i],
                                               'stock', qty_list[i])
                if type(incr_reply) == "table" and incr_reply.err then
                    return {false, "error_reverting_stock", item_ids[i], incr_reply.err}
                end
            end

            return {true, "revert_success", log_id_to_find}
        end
    end

    return {false, "log_id_not_found", log_id_to_find}
"""

check_and_update_stock_script = """
    -- Phase 1: Check if there's enough stock for all items
    for i=1, #KEYS do
        local item_id = tostring(KEYS[i])
        local quantity = tonumber(ARGV[i])

        -- Get current stock for the item
        -- Use `redis.pcall`, rather than `redis.call` since this one returns
        -- `redis.error_reply` indicating that there was a runtime error, rather than,
        -- for example, a key error.
        local current_stock = redis.pcall('HGET', item_id, 'stock')
        if not current_stock then
            return {false, "item_not_found", item_id}
        elseif type(current_stock) == "table" and current_stock.err then
            return {false, "database_error_during_check", item_id, current_stock.err}
        else
            current_stock = tonumber(current_stock)
        end

        -- Check if there's enough stock
        if current_stock < quantity then
            return {false, "not_enough_stock", item_id,
                tostring(current_stock), tostring(quantity)}
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

    local log_id = ARGV[#ARGV]
    local stream_key = "stock_update_log"
    redis.pcall('XADD', stream_key, 'MAXLEN', '~', 10000, '*',
        'log_id', log_id,
        'items', table.concat(KEYS, ','),
        'quantities', table.concat(ARGV, ',', 1, #KEYS)
    )

    return {true, "success"}
"""

batch_update_stock_script = """
    for i=1, #KEYS do
        redis.pcall('HSET', tostring(i-1), 'stock', ARGV[1], 'price', ARGV[2])
    end
"""


find_log_ids_script = """
    local log_id_1 = ARGV[1]
    local log_id_2 = ARGV[2]
    local stream_key = "stock_update_log"

    local found_1 = false
    local found_2 = false

    local entries = redis.pcall('XRANGE', stream_key, '-', '+')
    if type(entries) == "table" and entries.err then
        return {false, "stream_read_error", entries.err}
    end

    for _, entry in ipairs(entries) do
        local fields = entry[2]
        for i = 1, #fields, 2 do
            local field_name = fields[i]
            local field_value = fields[i+1]

            if field_name == "log_id" then
                if field_value == log_id_1 then
                    found_1 = true
                elseif field_value == log_id_2 then
                    found_2 = true
                end
            end
        end

        -- Early exit if both are found
        if found_1 and found_2 then
            break
        end
    end

    return {
        true,
        {log_id_1, found_1},
        {log_id_2, found_2}
    }
"""

check_and_update_stock = db.register_script(check_and_update_stock_script)
batch_update_stock = db.register_script(batch_update_stock_script)
revert_stock_update_script = db.register_script(revert_stock_update_script)
find_log_ids_script = db.register_script(find_log_ids_script)


class RabbitMQHandler:
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_HOST))

        self.channel = self.connection.channel()

        # Dead letter connection & channel
        self.dlx_connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_HOST))
        self.dlx_channel = self.dlx_connection.channel()

        # Declare queues
        self.channel.exchange_declare(DLX_EXCHANGE, "direct")

        self.channel.queue_declare(
            queue=STOCK_SERVICE_REQUESTS_QUEUE,
            arguments={
                "x-dead-letter-exchange": DLX_EXCHANGE,
                "x-dead-letter-routing-key": STOCK_DLX_KEY,
                "x-message-ttl": MESSAGE_TTL * 10000,
            },
        )

        self.dlx_channel.queue_declare(DEAD_LETTER_STOCK_QUEUE)
        self.dlx_channel.queue_bind(
            DEAD_LETTER_STOCK_QUEUE, DLX_EXCHANGE, STOCK_DLX_KEY
        )

    def dead_callback(self, ch, method, properties, body):
        message = json.loads(body.decode())
        log_id = message["log_id"]
        order_items_quantities = message["items"]
        order_type = message["type"]

        keys = list(order_items_quantities.keys())
        args = list(order_items_quantities.values()) + [str(uuid.uuid4())]

        if order_type == "compensation":
            target_entry_log = message.get("target_entry_log")
            if target_entry_log:
                result = find_log_ids_script(args=[log_id, target_entry_log])
                if result[0] and not result[2][1]:
                    app.logger.debug(
                        "There is no target entry for the rollback "
                        "and so it will be dropped"
                    )
                    self.dlx_channel.basic_ack(delivery_tag=method.delivery_tag)
                    return
            else:
                result = find_log_ids_script(args=[log_id, ""])

            if not result[0]:
                app.logger.debug("And error occurred with the database")
            else:
                if not result[1][1]:
                    rollback_result = check_and_update_stock(keys, args)
                    app.logger.debug(f"Rollback reattempted: {rollback_result}")
                else:
                    app.logger.debug(
                        "The target entry has been already rolled back "
                        "so the request will be droped"
                    )
        else:
            result = revert_stock_update_script(args=[log_id])
            app.logger.debug(f"Reverting an entry in the database: {result}")

        self.dlx_channel.basic_ack(delivery_tag=method.delivery_tag)

    def callback(self, ch, method, properties, body):
        """Callback function to process messages from RabbitMQ queue."""
        # Expected message type: {'item_id': id, 'quantity': n, ...}.
        message = json.loads(body.decode())
        order_id = message["order_id"]
        log_id = message["log_id"]
        order_items_quantities = message["items"]
        order_type = message["type"]
        if method.redelivered:
            self.channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return
        # Crash before an action happens happens
        # os._exit(1)

        # Crash before a rollback happens
        # if order_type == "compensation":
        #     os._exit(1)
        keys = list(order_items_quantities.keys())
        args = list(order_items_quantities.values()) + [log_id]

        try:
            result = check_and_update_stock(keys, args)
        except redis.exceptions.ConnectionError as e:
            response_message = (
                f"For order {order_id}, there were issues with the connection to the "
                + f"database:\n{e}"
            )
            self.publish_message(
                method,
                properties,
                response_message,
                400,
                order_id,
            )
            self.channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        status_of_result = result[0]

        if status_of_result:
            # In case there was enough stock for the entire order, then publish SUCCESS
            # message to the Order Checkout saga replies queue.
            if order_type == "action":
                # Crash after an action has been made
                # os._exit(1)
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
                # Crash after a rollback has happened
                # os._exit(1)
        else:
            # Handle different error cases.
            # Decode the received message since Lua returns bytes rather
            # than the string.
            error_type = result[1].decode("utf-8")
            error_item = result[2].decode("utf-8")
            match error_type:
                case "item_not_found":
                    response_message = f"For order {order_id}, item {error_item} was "
                    +"not found in the database."
                case "database_error_during_check" | "database_error_during_update":
                    error = result[3]
                    response_message = (
                        f"Order {order_id} could not be processed since there was an "
                        + f"error with the database.\n{error}"
                    )
                case "not_enough_stock":
                    current_stock = result[3].decode("utf-8")
                    requested_quantity = result[4].decode("utf-8")
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
        self.channel.basic_ack(delivery_tag=method.delivery_tag)

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

    def start_dead_consuming(self):
        self.dlx_channel.basic_consume(
            queue=DEAD_LETTER_STOCK_QUEUE, on_message_callback=self.dead_callback
        )
        app.logger.debug("Started listening to dead letter queue...")
        self.dlx_channel.start_consuming()

    def close_connection(self):
        self.connection.close()
        self.dlx_connection.close()


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


def start_dead_consumer():
    rabbitmq_handler.start_dead_consuming()


# Start RabbitMQ Consumer in a separate thread.
consumer_thread = threading.Thread(target=start_consumer, daemon=True)
dlq_consumer_thread = threading.Thread(target=start_dead_consumer, daemon=True)

consumer_thread.start()
dlq_consumer_thread.start()


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
    batch_update_stock([i for i in range(n)], [starting_stock, item_price])
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
