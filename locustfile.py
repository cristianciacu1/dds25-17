import random

from locust import HttpUser, SequentialTaskSet, constant, task

NUMBER_OF_ORDERS = 100_000
URL = "http://localhost:8000"

# replace the example urls and ports with the appropriate ones
ORDER_URL = URL
PAYMENT_URL = URL
STOCK_URL = URL


class CreateAndCheckoutOrder(SequentialTaskSet):
    @task
    def user_checks_out_order(self):
        order_id = random.randint(0, NUMBER_OF_ORDERS - 1)
        with self.client.post(
            f"{ORDER_URL}/orders/checkout/{order_id}",
            name="/orders/checkout/[order_id]",
            catch_response=True,
        ) as response:
            if 400 <= response.status_code < 500:
                response.failure(response.text)
            else:
                response.success()


class MicroservicesUser(HttpUser):
    # how much time a user waits (seconds) to run another TaskSequence (you could
    # also use between (start, end))
    wait_time = constant(1)
    # [SequentialTaskSet]: [weight of the SequentialTaskSet]
    tasks = {CreateAndCheckoutOrder: 100}
