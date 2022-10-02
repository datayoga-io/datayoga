# Processing strategies

Stream processing enables to process real-time data from a variety of sources. Unlike traditional batch ETL, stream processing deals with continuous processing of a never ending stream of data.

This introduces new challenges and opportunities for optimization to reduce the mean-time-to-process (MTTP) and increase throughput. Many considerations are taken into account when deciding on which processing strategy to use, based on the characteristics of the input sources, the data transformation, and the output targets. DataYoga's modular Transformation Jobs allow to mix processing strategies for each Transformation Step.

The examples below assume that records flow through a Job with 3 Steps, marked as A, B, and C. r1, r2, and r3 mark the data records in the dataset.
The Steps are defined as follows:

- Step A - takes 0.5s to complete
- Step B- takes 1s to complete
- Step C - takes 1s to complete

## Synchronous sequential processing

In sequential processing, each record (or a batch of records) passes through each of the Steps. The second record waits for the first to finish. In this example, each record would take 2.5 seconds to complete, showing a total of 7.5 seconds to process the dataset.

![sequential processing of records](./images/stream_process_sequential.png "Sequential processing")

## Continuous sequential processing

Continuous sequential processing allows to increase throughput by allowing each Step to start processing the next request once the current request's Step processing is complete. This relies on asynchronous processing and allows to parallelize the operation of all Steps while maintain strict order of processing. Only one record is processed per each Step at any given moment.

In our example, we can reduce the processing time of the dataset to 4.5 seconds.

![continuous processing of records](./images/stream_process_continuous.png "Continuous processing")

## Parallel processing

Parallel processing allows to process more than one record within a transformation Step at the same time. Each Step can have a different degree of parallelism. By default, Parallelism uses an asynchronous event loop to maximize performance. However, in case of CPU-bound activities, parallelism can also be performed using multiple OS processes. In case a Step reaches its maximum parallelism, backpressure will be applied to pause upstream Steps from producing additional activity until a slot frees up within the congested Step.
In this example, a parallel setting of 2 has been applied to steps B and C, while step A only processes one record at a time (parallelism of 1). These settings reduce the time to process the dataset to 3.5 seconds.

![parallel processing of records](./images/stream_process_parallel.png "Continuous processing")

## Sharded Parallel processing

Parallel processing can dramatically increase performance. However, if ordering of events is important, Parallel processing may cause issues due to race conditions and out-of-order events. For example, when processing change events, we may end up with an 'insert' operation that accidentally preceeds a 'delete' or vice versa.

Sharded Parallel Processing allows to define a sharding key that will ensure that all events relating to the same key will be processed by the same processing instance. This allows to maintain order of events per key, while still allowing parallel processing.

## Buffering

In some cases, batch processing may be more efficient than handling individual records. For example, bulk insert into relational database can be up to x100 more performant than individual inserts. In other cases, we encounter a limit of the number of calls we can perform (see also 'rate limit' below) and can group multiple calls in one batch call. In these cases, it is preferable to add a Buffering capability into the Job. Buffering groups together a group of records. The downstream Steps are presented with the group as the input.

In this example, a buffer size of 3 was applied, and is assumed that Step B and C can handle batches as well as individual records.

![buffering of records](./images/stream_process_buffer.png "Buffering records")

## Buffering with flush interval

When using buffering, we need to add a mechanism that will allow incomplete buffer to be sent out periodically to avoid a case where bufferred records linger for too long. Using a flush interval, buffers will be sent downstream every interval regardless of the size of the accumulated buffer.

In this example, a buffer size of 3 was applied, and a flush interval of 1 second.

![buffering of records](./images/stream_process_buffer.png "Buffering records with interval")

## Rate limit

Rate limit allows to set guards for the frequency of processing in a given time frame. This is useful, for example, in cases of working with external APIs to avoid creating a 'denial of service' or to meet API usage limits by the API provider.

The Rate limit strategy defines the number of requests per given time interval. For example, 5 requests a minute. When the limit is reached, processing for this Step will pause until the time period elapses to allow additional calls.

## Mix and match

The processing strategies can be mixed to fit the specific use case. For example, reading records from a Stream one by one, pushing into a parallel processor to perform a transformation, batched and fanned out to multiple processes to load into a relational database in bulk
