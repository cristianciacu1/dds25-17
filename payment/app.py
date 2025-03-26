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
MESSAGE_TTL = int(os.environ["MESSAGE_TTL"])
DLX_EXCHANGE = os.environ["DLX_EXCHANGE"]
DEAD_LETTER_PAYMENT_QUEUE = os.environ["DEAD_LETTER_PAYMENT_QUEUE"]
PAYMENT_DLX_KEY = os.environ["PAYMENT_DLX_KEY"]
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

    local available_funds = tonumber(redis.call('HGET', user_id, 'credit'))
    if not available_funds then
        return {false, "user_not_found", user_id}
    end

    if available_funds < total_cost then
        return {false, "not_enough_funds", user_id}
    end

    redis.call('HINCRBY', user_id, 'credit', -total_cost)

    local log_id = ARGV[#ARGV]
    local stream_key = "payment_update_log"
    redis.pcall('XADD', stream_key, 'MAXLEN', '~', 10000, '*',
        'log_id', log_id,
        'user_id', user_id,
        'total_cost', total_cost
    )

    return {true, "success"}
"""

revert_payment_update_script = '''
    local log_id_to_find = ARGV[1]
    local stream_key = "payment_update_log"

    -- Step 1: Read recent entries from the stream
    local entries = redis.pcall('XRANGE', stream_key, '-', '+')
    if type(entries) == "table" and entries.err then
        return {false, "stream_read_error", entries.err}
    end

    for _, entry in ipairs(entries) do
        local entry_id = entry[1]
        local fields = entry[2]

        local found_log_id = nil
        local user_id = nil
        local total_cost = nil

        -- Step 2: Parse fields to find log_id, user_id, and total_cost
        for i = 1, #fields, 2 do
            local field_name = fields[i]
            local field_value = fields[i+1]

            if field_name == "log_id" then
                found_log_id = field_value
            elseif field_name == "user_id" then
                user_id = field_value
            elseif field_name == "total_cost" then
                total_cost = tonumber(field_value)
            end
        end

        -- Step 3: If we find the matching log_id, revert by incrementing user credit
        if found_log_id == log_id_to_find then
            if not user_id or not total_cost then
                return {false, "log_data_malformed", log_id_to_find}
            end

            local incr_reply = redis.pcall('HINCRBY', user_id, "credit", total_cost)
            if type(incr_reply) == "table" and incr_reply.err then
                return {false, "error_reverting_credit", user_id, incr_reply.err}
            end

            return {true, "revert_success", log_id_to_find}
        end
    end

    return {false, "log_id_not_found", log_id_to_find}
'''

find_log_ids_script = """
    local log_id_1 = ARGV[1]
    local log_id_2 = ARGV[2]
    local stream_key = "payment_update_log"

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

revert_payment_update_script = db.register_script(revert_payment_update_script)
check_and_charge_user = db.register_script(check_and_charge_user_script)
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

        self.channel.queue_declare(queue=PAYMENT_SERVICE_REQUESTS_QUEUE, arguments={
                "x-dead-letter-exchange": DLX_EXCHANGE, 
                "x-dead-letter-routing-key": PAYMENT_DLX_KEY,
                "x-message-ttl": MESSAGE_TTL * 10000
        })

        self.dlx_channel.queue_declare(DEAD_LETTER_PAYMENT_QUEUE)
        self.dlx_channel.queue_bind(DEAD_LETTER_PAYMENT_QUEUE, DLX_EXCHANGE, PAYMENT_DLX_KEY)

    def dead_callback(self, ch, method, properties, body):
        message = json.loads(body.decode())
        user_id = message["user_id"]
        amount = message["total_cost"]
        order_id = message["order_id"]
        order_type = message["type"]
        log_id = message["log_id"]
        
        
        keys = list(user_id)
        args = [amount, str(uuid.uuid4())]
        
        if order_type == 'compensation':
            target_entry_log = message.get("target_entry_log")
            if target_entry_log:
                result = find_log_ids_script(args=[log_id, target_entry_log])
                if result[0] and not result[2][1]:
                    app.logger.debug(f"There is no target entry for the rollback and so it will be droped")
                    self.dlx_channel.basic_ack(delivery_tag=method.delivery_tag)
                    return
            else:
                result = find_log_ids_script(args=[log_id, ""])
            
            if not result[0]:
                app.logger.debug(f"And error occurred with the database")
            else:
                if not result[1][1]:
                    rollback_result = check_and_charge_user(keys, args)
                    app.logger.debug(f"Rollback reattempted: {rollback_result}")
                else:
                    app.logger.debug(f"The target entry has been already rolled back so the request will be droped")
        else:
            result = revert_payment_update_script(args=[log_id])
            app.logger.debug(f"Reverting an entry in the database: {result}")

        self.dlx_channel.basic_ack(delivery_tag=method.delivery_tag)

    def callback(self, ch, method, properties, body):
        """Callback function to process messages from RabbitMQ queue."""
        # Expected message type: {'user_id': int, 'total_cost': int}.
        message = json.loads(body.decode())
        user_id = message["user_id"]
        amount = message["total_cost"]
        order_id = message["order_id"]
        order_type = message["type"]
        log_id = message["log_id"]

        if method.redelivered:
            self.channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        ### Crash before an action happens happens
        # os._exit(1)

        ### Crash before a rollback happens
        #if order_type == "compensation":
        #    os._exit(1)

        result = check_and_charge_user([user_id], [amount] + [log_id])

        status_of_result = result[0]

        if status_of_result:
            # If successful and a saga action was performed then publish SUCCESS
            # event to the order checkout saga replies queue.
            if order_type == "action":
                ### Crash after an action has been made
                # os._exit(1)
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
                ### Crash after a rollback has happened
                # os._exit(1)
        else:
            # Handle different error cases.
            error_type = result[1]

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
                case _:
                    response_message = (
                        f"When charging user {user_id} for order {order_id}, an "
                        + f"unknown error occurred: {error_type}"
                    )
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
            "type": "payment",
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

    def start_dead_consuming(self):
        self.dlx_channel.basic_consume(
            queue=DEAD_LETTER_PAYMENT_QUEUE, on_message_callback=self.dead_callback
        )
        app.logger.debug("Started listening to dead letter queue...")
        self.dlx_channel.start_consuming()

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

def start_dead_consumer():
    rabbitmq_handler.start_dead_consuming()


# Start RabbitMQ Consumer in a separate thread.
consumer_thread = threading.Thread(target=start_consumer, daemon=True)
dlq_consumer_thread = threading.Thread(target=start_dead_consumer, daemon=True)

consumer_thread.start()
dlq_consumer_thread.start()


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
    import logging

    gunicorn_logger = logging.getLogger("gunicorn.error")
    if gunicorn_logger.handlers:
        # Copy Gunicorn's handlers to Flask's app.logger
        for handler in gunicorn_logger.handlers:
            app.logger.addHandler(handler)
    app.logger.setLevel(logging.DEBUG)  # or gunicorn_logger.level