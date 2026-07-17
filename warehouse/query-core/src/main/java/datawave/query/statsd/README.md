# Benchmark Report
### QueryStatsDClientBenchmark.java
- Benchmarked class: QueryStatsDClient.java
- Counter Type: AtomicInteger
- Benched Methods:
  - next()
  - flush()
  - getSize()

### Results: 
- Total time: 00:03:21

| Benchmark | (iterations) | Mode | Cnt | Score | Error | Units |
|---|---|---|---|---|---|---|
| QueryStatsDClientBenchmark.benchFlush | 1 | thrpt | 3 | 11.430 | ± 2.759 | ops/us |
| QueryStatsDClientBenchmark.benchFlush | 10 | thrpt | 3 | 1.243 | ± 0.317 | ops/us |
| QueryStatsDClientBenchmark.benchFlush | 50 | thrpt | 3 | 0.241 | ± 0.008 | ops/us |
| QueryStatsDClientBenchmark.benchFlush | 100 | thrpt | 3 | 0.118 | ± 0.036 | ops/us |
| QueryStatsDClientBenchmark.benchFlush | 300 | thrpt | 3 | 0.042 | ± 0.007 | ops/us |
| QueryStatsDClientBenchmark.benchGetSize | 1 | thrpt | 3 | 7.512 | ± 0.614 | ops/us |
| QueryStatsDClientBenchmark.benchGetSize | 10 | thrpt | 3 | 0.935 | ± 0.070 | ops/us |
| QueryStatsDClientBenchmark.benchGetSize | 50 | thrpt | 3 | 0.188 | ± 0.067 | ops/us |
| QueryStatsDClientBenchmark.benchGetSize | 100 | thrpt | 3 | 0.085 | ± 0.037 | ops/us |
| QueryStatsDClientBenchmark.benchGetSize | 300 | thrpt | 3 | 0.037 | ± 0.004 | ops/us |
| QueryStatsDClientBenchmark.benchNext | 1 | thrpt | 3 | 5.159 | ± 0.838 | ops/us |
| QueryStatsDClientBenchmark.benchNext | 10 | thrpt | 3 | 0.643 | ± 0.341 | ops/us |
| QueryStatsDClientBenchmark.benchNext | 50 | thrpt | 3 | 0.108 | ± 0.025 | ops/us |
| QueryStatsDClientBenchmark.benchNext | 100 | thrpt | 3 | 0.055 | ± 0.013 | ops/us |
| QueryStatsDClientBenchmark.benchNext | 300 | thrpt | 3 | 0.015 | ± 0.012 | ops/us |
