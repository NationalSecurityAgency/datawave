package datawave.query.statsd;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class QueryStatsDClientAtomicIntegerBenchmark {

    private QueryStatsDClientAtomicInteger qc;

    @Param({"0", "100", "5000"})
    public int maxCacheSize;

    // Reporting label placeholder to allow threads to display in benchmark summary chart
    @Param({"1"})
    public int threads;

    @Setup(Level.Trial)
    public void setUp() {
        qc = new QueryStatsDClientAtomicInteger("id123", "localhost", 3000, maxCacheSize);
    }

    @Benchmark
    public int benchNext() {
        return qc.next();
    }

    @Benchmark
    public boolean benchFlush() {
        return qc.flush();
    }

    @Benchmark
    public int benchGetSize() {
        return qc.getSize();
    }

    public static void main(String[] args) throws RunnerException {
        String[] threadList = new String[]{"1", "4", "14"};

        OptionsBuilder builder = new OptionsBuilder();

        // Add values to threads param
        for (String t : threadList) {
            builder.param("threads", t);
            builder.threads(Integer.parseInt(t));
        }

        builder.include(QueryStatsDClientAtomicIntegerBenchmark.class.getSimpleName())
                .forks(2)
                .build();

        new Runner(builder.build()).run();
    }
}