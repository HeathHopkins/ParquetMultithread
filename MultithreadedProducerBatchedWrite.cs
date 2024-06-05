
using System.Threading.Tasks.Dataflow;
using Parquet;
using Parquet.Serialization;

namespace ParquetMultiThreaded
{
    public class MultithreadedProducerBatchedWrite
    {
        public static async Task<(long fileSizeBytes, TimeSpan runTime)> RunAsync()
        {
            const int maxDegreeOfParallelism = 10;
            const int writeBatchSize = 500;

            Console.WriteLine("Starting multithreaded producer with batched writer ...");
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();

            using var fileStream = new FileStream($"{Guid.NewGuid()}.parquet", FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite, 4096, FileOptions.DeleteOnClose);

            // write to parquet in batches of 500 items
            var batchBlock = new BatchBlock<Item>(batchSize: writeBatchSize);
            var consumerTask = ConsumerAsync(batchBlock, fileStream);

            // suppress warning about not awaiting the producer task
            // the consumer task is awaited, so the producer task will be awaited
            #pragma warning disable 4014
            ProducerAsync(batchBlock, maxDegreeOfParallelism);
            #pragma warning restore 4014

            await consumerTask;
            await fileStream.FlushAsync();
            stopwatch.Stop();

            Console.WriteLine("Done");
            return (fileStream.Length, stopwatch.Elapsed);
        }

        static async Task ConsumerAsync(BatchBlock<Item> batchBlock, FileStream fileStream, CancellationToken cancellationToken = default)
        {
            var firstWrite = true;
            while (await batchBlock.OutputAvailableAsync())
            {
                var items = await batchBlock.ReceiveAsync();
                await ParquetSerializer.SerializeAsync(items, fileStream, new ParquetSerializerOptions
                {
                    CompressionMethod = CompressionMethod.Snappy,
                    CompressionLevel = System.IO.Compression.CompressionLevel.Optimal,
                    Append = !firstWrite,
                }, cancellationToken);
                firstWrite = false;
            }
            await fileStream.FlushAsync(cancellationToken);
        }

        static async Task ProducerAsync(BatchBlock<Item> target, int maxDegreeOfParallelism)
        {
            var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = maxDegreeOfParallelism };
            var indexes = Enumerable.Range(0, 1000);
            await Parallel.ForEachAsync(indexes, parallelOptions, async (index, cancellationToken) =>
            {
                Console.WriteLine($"Processing item {index}");

                // simulate work
                await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);

                target.Post(new Item
                {
                    index = index,
                    name = $"item {index}"
                });
            });

            target.Complete();
        }

        public record Item
        {
            public int index { get; init; }
            public string? name { get; init; }
        }
    }
}