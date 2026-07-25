# Benchmark Result Notes:

* Benchmarks were conducted using Java Microbenchmark Harness (JMH).

* Mode: Throughput - measures the total number of operations completed per unit of time.

* Cnt: Stands for count. JMH ran 10 separate measurement iterations for each parameter combination, averaged those 10 runs together to calculate the Score, and used the variance between those 10 runs to calculate Margin Of Error. This is displayed in the separate charts.

* Using different levels of maxCacheSize causes the flushStats() method of QueryStatsDClient to execute at different amounts, which also affects the contention level like using different amounts of threads.

* ops: The number of benchmark method calls executed (operations).
  * us: Microseconds
  * Score of 1 ops/us = 1,000,000 operations per second

* QueryStatsDClientAtomicInteger is the same as QueryStatsDClient, except the methods have been changed to return a value to prevent JMH from optimizing away the computation in the benchmark classes.

* QueryStatsDClientLongAdder has been changed in the same way but has also been converted to use LongAdder instead of AtomicInteger.


### AtomicInteger vs. LongAdder Performance and Variability Delta

| Benchmark | maxCacheSize | threads | AtomicInteger Score | LongAdder Score | Score Delta | AtomicInteger Error | LongAdder Error | Error Delta | Units |
|---|---|---|---|---|---|---|---|---|---|
| benchFlush | 0 | 1 | 7.506 | 6.906 | 0.600 | ± 1.542 | ± 3.008 | 1.466 | ops/us |
| benchFlush | 0 | 4 | 7.591 | 6.405 | 1.186 | ± 1.511 | ± 2.654 | 1.143 | ops/us |
| benchFlush | 0 | 14 | 6.493 | 8.281 | 1.788 | ± 2.622 | ± 0.447 | 2.175 | ops/us |
| benchFlush | 100 | 1 | 6.639 | 6.814 | 0.175 | ± 2.835 | ± 2.786 | 0.049 | ops/us |
| benchFlush | 100 | 4 | 6.568 | 6.618 | 0.050 | ± 2.542 | ± 3.023 | 0.481 | ops/us |
| benchFlush | 100 | 14 | 6.851 | 6.367 | 0.484 | ± 2.194 | ± 2.678 | 0.484 | ops/us |
| benchFlush | 5000 | 1 | 8.271 | 4.758 | 3.513 | ± 0.225 | ± 0.073 | 0.152 | ops/us |
| benchFlush | 5000 | 4 | 8.471 | 4.948 | 3.523 | ± 0.123 | ± 0.420 | 0.297 | ops/us |
| benchFlush | 5000 | 14 | 6.971 | 4.648 | 2.323 | ± 2.363 | ± 0.142 | 2.221 | ops/us |
| benchGetSize | 0 | 1 | 4.924 | 5.068 | 0.144 | ± 0.099 | ± 0.450 | 0.351 | ops/us |
| benchGetSize | 0 | 4 | 5.340 | 5.016 | 0.324 | ± 0.184 | ± 0.391 | 0.207 | ops/us |
| benchGetSize | 0 | 14 | 5.363 | 5.040 | 0.323 | ± 0.176 | ± 0.268 | 0.092 | ops/us |
| benchGetSize | 100 | 1 | 5.069 | 4.674 | 0.395 | ± 0.329 | ± 0.030 | 0.299 | ops/us |
| benchGetSize | 100 | 4 | 5.300 | 5.012 | 0.288 | ± 0.247 | ± 0.313 | 0.066 | ops/us |
| benchGetSize | 100 | 14 | 5.003 | 5.000 | 0.003 | ± 0.070 | ± 0.127 | 0.057 | ops/us |
| benchGetSize | 5000 | 1 | 5.142 | 4.945 | 0.197 | ± 0.298 | ± 0.246 | 0.052 | ops/us |
| benchGetSize | 5000 | 4 | 4.974 | 4.877 | 0.097 | ± 0.441 | ± 0.121 | 0.320 | ops/us |
| benchGetSize | 5000 | 14 | 5.075 | 4.999 | 0.076 | ± 0.236 | ± 0.479 | 0.243 | ops/us |
| benchNext | 0 | 1 | 1.231 | 1.058 | 0.173 | ± 0.080 | ± 0.157 | 0.077 | ops/us |
| benchNext | 0 | 4 | 1.216 | 1.186 | 0.030 | ± 0.132 | ± 0.237 | 0.105 | ops/us |
| benchNext | 0 | 14 | 1.249 | 1.116 | 0.133 | ± 0.108 | ± 0.139 | 0.031 | ops/us |
| benchNext | 100 | 1 | 3.803 | 3.413 | 0.390 | ± 0.102 | ± 0.065 | 0.037 | ops/us |
| benchNext | 100 | 4 | 3.825 | 3.283 | 0.542 | ± 0.097 | ± 0.141 | 0.044 | ops/us |
| benchNext | 100 | 14 | 3.936 | 3.413 | 0.523 | ± 0.168 | ± 0.068 | 0.100 | ops/us |
| benchNext | 5000 | 1 | 3.714 | 3.738 | 0.024 | ± 0.030 | ± 0.091 | 0.061 | ops/us |
| benchNext | 5000 | 4 | 3.783 | 3.734 | 0.049 | ± 0.209 | ± 0.096 | 0.113 | ops/us |
| benchNext | 5000 | 14 | 3.880 | 3.844 | 0.036 | ± 0.318 | ± 0.159 | 0.159 | ops/us |

# Analysis:

At varying levels of contention, we can see that the results are overall pretty close for the performance of QueryStatsDClient with an implementation that uses AtomicInteger vs using LongAdder.

- benchNext():
  - Close Performance: Both versions perform almost identically under moderate contention (∼3.8 ops/us) and
    high contention (∼1.2 ops/us) setups
  - LongAdder Performance Negation: LongAdder can be fast by using multiple threads to increment separate cells in memory. Since next() is designed to increment and return a value, QueryStatsDClientLongAdder must use increment() and the sum() operation, and sum() makes the method more expensive.


- benchFlush() & Score Variability:
  - Max Contention: At peak contention (maxCacheSize = 0, threads = 14), LongAdder performs better in benchFlush()
    with 8.281 ops/us compared to AtomicInteger's 6.493 ops/us.
  - AtomicInteger Score Variability: There is significantly higher score variability under moderate-to-high
    contention. For example, at maxCacheSize = 5000, threads = 14, benchFlush() hit 6.971±2.363 ops/us
    for the AtomicInteger version, while the LongAdder version stayed much more stable at 4.648±0.142 ops/us.
  - Explanation: This higher error margin comes from multiple threads fighting over the exact same memory address
    using CAS loops. When threads collide and constantly retry, throughput gets erratic.

# Conclusion:
AtomicInteger seems like the better option for this use case and system. Since the method next() needs to return a value and will be called frequently, LongAdder's design isn't as effective.

# Separate Charts:
## QueryStatsDClientAtomicInteger Benchmark Results:

### Total time: 00:19:43

| Benchmark | (maxCacheSize) | (threads) | Mode | Cnt | Score | Error | Units |
|---|---|---|---|---|---|---|---|
| benchFlush | 0 | 1 | thrpt | 10 | 7.506 | ± 1.542 | ops/us |
| benchFlush | 0 | 4 | thrpt | 10 | 7.591 | ± 1.511 | ops/us |
| benchFlush | 0 | 14 | thrpt | 10 | 6.493 | ± 2.622 | ops/us |
| benchFlush | 100 | 1 | thrpt | 10 | 6.639 | ± 2.835 | ops/us |
| benchFlush | 100 | 4 | thrpt | 10 | 6.568 | ± 2.542 | ops/us |
| benchFlush | 100 | 14 | thrpt | 10 | 6.851 | ± 2.194 | ops/us |
| benchFlush | 5000 | 1 | thrpt | 10 | 8.271 | ± 0.225 | ops/us |
| benchFlush | 5000 | 4 | thrpt | 10 | 8.471 | ± 0.123 | ops/us |
| benchFlush | 5000 | 14 | thrpt | 10 | 6.971 | ± 2.363 | ops/us |
| benchGetSize | 0 | 1 | thrpt | 10 | 4.924 | ± 0.099 | ops/us |
| benchGetSize | 0 | 4 | thrpt | 10 | 5.340 | ± 0.184 | ops/us |
| benchGetSize | 0 | 14 | thrpt | 10 | 5.363 | ± 0.176 | ops/us |
| benchGetSize | 100 | 1 | thrpt | 10 | 5.069 | ± 0.329 | ops/us |
| benchGetSize | 100 | 4 | thrpt | 10 | 5.300 | ± 0.247 | ops/us |
| benchGetSize | 100 | 14 | thrpt | 10 | 5.003 | ± 0.070 | ops/us |
| benchGetSize | 5000 | 1 | thrpt | 10 | 5.142 | ± 0.298 | ops/us |
| benchGetSize | 5000 | 4 | thrpt | 10 | 4.974 | ± 0.441 | ops/us |
| benchGetSize | 5000 | 14 | thrpt | 10 | 5.075 | ± 0.236 | ops/us |
| benchNext | 0 | 1 | thrpt | 10 | 1.231 | ± 0.080 | ops/us |
| benchNext | 0 | 4 | thrpt | 10 | 1.216 | ± 0.132 | ops/us |
| benchNext | 0 | 14 | thrpt | 10 | 1.249 | ± 0.108 | ops/us |
| benchNext | 100 | 1 | thrpt | 10 | 3.803 | ± 0.102 | ops/us |
| benchNext | 100 | 4 | thrpt | 10 | 3.825 | ± 0.097 | ops/us |
| benchNext | 100 | 14 | thrpt | 10 | 3.936 | ± 0.168 | ops/us |
| benchNext | 5000 | 1 | thrpt | 10 | 3.714 | ± 0.030 | ops/us |
| benchNext | 5000 | 4 | thrpt | 10 | 3.783 | ± 0.209 | ops/us |
| benchNext | 5000 | 14 | thrpt | 10 | 3.880 | ± 0.318 | ops/us |

<br><br>

## QueryStatsDClientLongAdder Benchmark Results:

### Total time: 00:19:42

| Benchmark | maxCacheSize | threads | Mode | Cnt | Score | Error | Units |
|---|---|---|---|---|---|---|---|
| benchFlush | 0 | 1 | thrpt | 10 | 6.906 | ± 3.008 | ops/us |
| benchFlush | 0 | 4 | thrpt | 10 | 6.405 | ± 2.654 | ops/us |
| benchFlush | 0 | 14 | thrpt | 10 | 8.281 | ± 0.447 | ops/us |
| benchFlush | 100 | 1 | thrpt | 10 | 6.814 | ± 2.786 | ops/us |
| benchFlush | 100 | 4 | thrpt | 10 | 6.618 | ± 3.023 | ops/us |
| benchFlush | 100 | 14 | thrpt | 10 | 6.367 | ± 2.678 | ops/us |
| benchFlush | 5000 | 1 | thrpt | 10 | 4.758 | ± 0.073 | ops/us |
| benchFlush | 5000 | 4 | thrpt | 10 | 4.948 | ± 0.420 | ops/us |
| benchFlush | 5000 | 14 | thrpt | 10 | 4.648 | ± 0.142 | ops/us |
| benchGetSize | 0 | 1 | thrpt | 10 | 5.068 | ± 0.450 | ops/us |
| benchGetSize | 0 | 4 | thrpt | 10 | 5.016 | ± 0.391 | ops/us |
| benchGetSize | 0 | 14 | thrpt | 10 | 5.040 | ± 0.268 | ops/us |
| benchGetSize | 100 | 1 | thrpt | 10 | 4.674 | ± 0.030 | ops/us |
| benchGetSize | 100 | 4 | thrpt | 10 | 5.012 | ± 0.313 | ops/us |
| benchGetSize | 100 | 14 | thrpt | 10 | 5.000 | ± 0.127 | ops/us |
| benchGetSize | 5000 | 1 | thrpt | 10 | 4.945 | ± 0.246 | ops/us |
| benchGetSize | 5000 | 4 | thrpt | 10 | 4.877 | ± 0.121 | ops/us |
| benchGetSize | 5000 | 14 | thrpt | 10 | 4.999 | ± 0.479 | ops/us |
| benchNext | 0 | 1 | thrpt | 10 | 1.058 | ± 0.157 | ops/us |
| benchNext | 0 | 4 | thrpt | 10 | 1.186 | ± 0.237 | ops/us |
| benchNext | 0 | 14 | thrpt | 10 | 1.116 | ± 0.139 | ops/us |
| benchNext | 100 | 1 | thrpt | 10 | 3.413 | ± 0.065 | ops/us |
| benchNext | 100 | 4 | thrpt | 10 | 3.283 | ± 0.141 | ops/us |
| benchNext | 100 | 14 | thrpt | 10 | 3.413 | ± 0.068 | ops/us |
| benchNext | 5000 | 1 | thrpt | 10 | 3.738 | ± 0.091 | ops/us |
| benchNext | 5000 | 4 | thrpt | 10 | 3.734 | ± 0.096 | ops/us |
| benchNext | 5000 | 14 | thrpt | 10 | 3.844 | ± 0.159 | ops/us |