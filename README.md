# Smart Kafka Disk Balancer
## Implementation
We would like to have a script that will to the following:
1. Get all Logdirs on a specific kafka broker.
2. Wait 60 Seconds.
3. Get all Logdirs on the same specific kafka broker.
4. Create new field under each replica to understand the mbps rate (IO writes).
5. Create 2 types of strategies, `DiskUsageStrategy` and `DiskCapacityStrategy`.
6. Let the user choose hes designated strategy and execute a plan.
7. should create a Class that will execute the strategy with throttling.

