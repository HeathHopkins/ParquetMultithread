# Parquet.Net multithreaded writing

These are examples and benchmarks of writing to a single parquet file from multiple threads using the C# Parquet.Net library.

## Benchmarks

Each strategy used 1,000 items each with a 500 ms work delay over 10 threads. The parquet file was written to the local disk.

| Strategy                                                                       | Size (MB) | Run Time (sec) |
| ------------------------------------------------------------------------------ | --------- | -------------- |
| [Multithreaded Producer - Batched Write](MultithreadedProducerBatchedWrite.cs) | 0.0051    | 50.1116        |
| [Multithreaded with Single Writer](MultithreadedSingleWriter.cs)               | 0.2689    | 50.5054        |

## Multithreaded Producer - Batched Write

This strategy uses a single writer and multiple producers. Each producer writes to a shared queue. The writer reads from the queue and writes to the parquet file. The writer writes in batches to the parquet file.

The parquet file's size is smaller because Parquet.Net's `ParquetSerializer.SerializeAsync()` method can compress the rows better when writing in batches.

## Multithreaded with Single Writer

This pattern assumes that you have many asynchronous tasks that are producing data that you want to write to a single parquet file. If your use case allows for it, you may be better off batching the data into the number of simultaneous threads based on your CPU count and have each thread write to a separate parquet file. This will allow you to take advantage parallelism while still getting good compression.
