using ConsoleTables;
using ParquetMultiThreaded;

var benchmarkTable = new ConsoleTable("Strategy", "Size (MB)", "Run Time (sec)");

{
    var (fileSizeBytes, runTime) = await MultithreadedProducerBatchedWrite.RunAsync();
    benchmarkTable.AddRow(
        "Multithreaded Producer - Batched Write", 
        (fileSizeBytes / 1024d / 1024d).ToString("N4"),
        runTime.TotalSeconds.ToString("N4")
    );
}

{
    var (fileSizeBytes, runTime) = await MultithreadedSingleWriter.RunAsync();
    benchmarkTable.AddRow(
        "Multithreaded with Single Writer", 
        (fileSizeBytes / 1024d / 1024d).ToString("N4"),
        runTime.TotalSeconds.ToString("N4")
    );
}

benchmarkTable.Write(Format.MarkDown);