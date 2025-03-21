# Details about our implementation
The goal of this project is to design the architecture and define the transaction protocol for a generic E-Commerce system composed of three parts: Order, Payment, and Stock. We propose to leverage an **event-driven microservices architecture**, as our focus for this system is to achieve **high performance** while still guaranteeing **eventual consistency**. Each of the three microservices manages its own database system, and interservice communication is performed asynchronously. The figure below illustrates the proposed architecture.

<img src="resources/dds_architecture.png" alt="Architecture for the system, group 17" width="600"/>

## Number of replicas
We decided to replicate our services to distribute the load over multiple workers with the goal of achieving higher throughput. Specifically, we have 4 replicas of the Order Service and 2 replicas each for Stock and Payment Services.

The reason behind the different number of replicas per service is the actual load. We observed that setting each service to be replicated 4 times yielded a worse result than the current setup (i.e. lower RPS and higher response time for 10.000 users checking out simultaneously).

## Changing the load balancer
While performing stress experiments, we saw that the response time increased at a slower than linear pace. Yet, after achieving an RPS of around 5.000, we saw a significant increase in the worst case response time (over 20.000ms). Additionally, we measured the latency of the checkout procedure and resulted in a surprising value - it ranges between 2 and 20ms, depending on the load, yet the actual response time was significantly higher. After some investigations, we came to the conclusion that the Nginx load balancer could not handle the high load. Did some research and [found out](https://www.loggly.com/blog/benchmarking-5-popular-load-balancers-nginx-haproxy-envoy-traefik-and-alb/) that the [Envoy Proxy](https://www.envoyproxy.io/) promises to provide better performance than Nginx. With the new load balancer in place, we achieve higher RPS, while keeping the response time relatively constant (after the number of users has stabilized).

## Performance of the system
Regarding the performance, our system achieves high throughput with relatively low response time, while still achieving 100% consistency. Specifically, when runnning Locust with 8 workers, while the system runs the configuration described above, we achieve an average of ~6.200 RPS with a median response time of 18ms. More detailed statistics on the performance of our system can be found [here](resources/Locust_2025-03-21-18h54_locustfile.py_http___master_8089.html).

## Locust with multiple workers
We saw that running Locust with only one worker cannot measure properly the performance of the system. Thus, we ran Locust with multiple workers by running an image of Locust with the docker-compose responsible for the system. 

Once Locust and the system are running, you can access Locust's dashboard at: `http://0.0.0.0:8089/?tab=charts`.

## docker-compose (local development)

After coding the REST endpoint logic run `docker-compose up --build` in the base folder to test if your logic is correct
(you can use the provided tests in the `\test` folder and change them as you wish). 

***Requirements:*** You need to have docker and docker-compose installed on your machine. 
