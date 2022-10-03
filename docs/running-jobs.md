---
nav_order: 7
---

# Running Jobs

The DataYoga runner reads and processes job definitions.

## DataYoga Docker Runner

## Performance

DataYoga is both multiprocess and multithreaded. It uses a fan-out streaming approach to distribute load between multiple workers for maximum throughput. Backpressure is handled to stop reading from source when the target becomes congested.
