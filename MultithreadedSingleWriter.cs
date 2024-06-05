
using Parquet;
using Parquet.Serialization;

namespace ParquetMultiThreaded
{
    public class MultithreadedSingleWriter
    {
        public static async Task<(long fileSizeBytes, TimeSpan runTime)> RunAsync()
        {
            Console.WriteLine("Starting single writer...");
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            var items = Enumerable.Range(0, 1000)
                .Select(i => new Item
                {
                    index = i,
                    name = $"item {i}"
                });

            using var fileStream = new FileStream($"{Guid.NewGuid()}.parquet", FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite, 4096, FileOptions.DeleteOnClose);

            // first write to set headers/footers in parquet file
            var emptyItems = new List<Item>();
            await ParquetSerializer.SerializeAsync(emptyItems, fileStream, new ParquetSerializerOptions
            {
                CompressionMethod = CompressionMethod.Snappy,
                CompressionLevel = System.IO.Compression.CompressionLevel.Optimal,
            });

            /*
                Caveat: 
                    The appended file size is larger than file size if you passed all of the items to 
                    ParquetSerializer.SerializeAsync at once.  Batching into larger groups will reduce
                    the file size.
            */

            // work on multiple threads, but only allow a single writer at a time
            using var semaphore = new SemaphoreSlim(1, 1);
            var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = 10 };
            await Parallel.ForEachAsync(items, parallelOptions, async (item, cancellationToken) =>
            {
                Console.WriteLine($"Processing item {item.index}");

                // simulate work
                await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);

                // wait for the lock to write
                await semaphore.WaitAsync(cancellationToken);
                try
                {
                    await ParquetSerializer.SerializeAsync(new[] { item }, fileStream, new ParquetSerializerOptions
                    {
                        CompressionMethod = CompressionMethod.Snappy,
                        CompressionLevel = System.IO.Compression.CompressionLevel.Optimal,
                        Append = true,
                    }, cancellationToken);
                }
                finally
                {
                    semaphore.Release();
                }
            });

            await fileStream.FlushAsync();
            stopwatch.Stop();

            Console.WriteLine("Done");
            return (fileStream.Length, stopwatch.Elapsed);
        }

        record Item
        {
            public int index { get; init; }
            public string? name { get; init; }
        }
    }
}