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
    public AtomicInteger benchAtomicIntegerIncrement() {
        for (int i = iterations; i > 0; i--) {
            atomicInteger.incrementAndGet();

        }

        return atomicInteger;
    }

    @Benchmark
    public AtomicInteger benchAtomicIntegerAdd5() {
        for (int i = 50; i > 0; i--) {
            atomicInteger.addAndGet(5);
        }
        return atomicInteger;
    }

    @Benchmark
    public LongAdder benchLongAdderIncrement(){
        for (int i = iterations; i > 0; i--) {
            longAdder.increment();
        }
        return longAdder;
    }

    @Benchmark
    public LongAdder benchLongAdderAdd5(){
        for (int i = 50; i > 0; i--) {
            longAdder.add(5);
            longAdder.sum();
        }
        return longAdder;
    }

    public static void main(String[] args) throws RunnerException {
        int[] threadCounts = {1,2,4,6,8,10,12,14};

        for(int t: threadCounts){
            System.out.println("===========================================");
            System.out.println("Thread Count: "+ t);
            System.out.println("===========================================");
            Options opt = new OptionsBuilder().include(BenchmarkEx.class.getSimpleName())
                    .threads(t).
                    forks(1).build();
            new Runner(opt).run();
        }


    }
}

