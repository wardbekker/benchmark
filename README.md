# Spark Benchmark Tool

This tool is inspired by the DFSIO benchmark for Hadoop mapreduce. It will test the write throughput to HDFS using the Spark framework.

## Getting the Spark Jar

Download the Spark Jar from here: https://github.com/wardbekker/benchmark/releases/download/v0.1/benchmark-1.0-SNAPSHOT-jar-with-dependencies.jar

It's build for Spark 1.6.2 / Scala 2.10.5

## Submit args explained:

`<file/partitions>` : should ideally be equal to recommended spark.default.parallelism (cores x instances)
`<bytes_per_file>` : should fit in memory: for example: 90000000 
`<write_repetitions>` : no of re-writing of the test RDD to disk. benchmark will be averaged.

```sh
spark-submit --class org.ward.Benchmark  --master yarn --deploy-mode cluster --num-executors X --executor-cores Y --executor-memory Z target/benchmark-1.0-SNAPSHOT-jar-with-dependencies.jar <files/partitions> <bytes_per_file> <write_repetitions>
```

## CLI Example for 12 workers with 30GB mem per node:

This command will write out the generated RDD 10 times, and will calculate an aggregate throughput over it.

```sh
spark-submit --class org.ward.Benchmark  --master yarn --deploy-mode cluster --num-executors 60 --executor-cores 3 --executor-memory 4G target/benchmark-1.0-SNAPSHOT-jar-with-dependencies.jar 180 90000000 10
```

## Retrieving benchmark results:

Retrieve the benchmark results by running:

```sh
yarn logs -applicationId <application_id> | grep 'Benchmark' 
```

for example:    

```
Benchmark: Total volume         : 81000000000 Bytes
Benchmark: Total write time     : 74.979 s
Benchmark: Aggregate Throughput : 1.08030246E9 Bytes per second
```
