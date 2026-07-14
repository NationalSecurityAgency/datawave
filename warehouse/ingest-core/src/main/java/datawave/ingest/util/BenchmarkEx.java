package datawave.ingest.util;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

// Throughput: Number of operations per time unit
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)

// Number of threads executing the benchmark concurrently
@Threads(14)

@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)

// Actual measurement iterations
@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)

// Number of times to spawn a new JVM process for the benchmark
@Fork(value = 1, warmups = 1)

@State(Scope.Benchmark)

/**
 * Compare performance of AtomicInteger operations vs LongAdder operations
 */
public class BenchmarkEx {
    /*
    Possible test cases:
    - Increment counter
    - Increment counter and get value
     */

    @Param({ "100", "500", "1000", "2500", "5000" })
    public int iterations;

    public AtomicInteger atomicInteger;

    public LongAdder longAdder;

    @Setup(Level.Trial)
    public void setUp() {
        atomicInteger = new AtomicInteger();
        longAdder = new LongAdder();
    }

    @Benchmark
    public void benchAtomicInteger() {
        for (int i = iterations; i > 0; i--) {
            atomicInteger.incrementAndGet();
        }
    }

    @Benchmark
    public void benchLongAdder(){
        for (int i = iterations; i > 0; i--) {
            longAdder.increment();
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder().include(BenchmarkEx.class.getSimpleName()).build();
        new Runner(opt).run();
    }
}