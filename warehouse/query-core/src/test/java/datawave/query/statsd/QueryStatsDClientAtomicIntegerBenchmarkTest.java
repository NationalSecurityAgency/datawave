package datawave.query.statsd;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class QueryStatsDClientAtomicIntegerBenchmarkTest {

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
        String[] threadList = new String[] {"1", "4", "14"};

        OptionsBuilder builder = new OptionsBuilder();

        // Add values to threads param
        for (String t : threadList) {
            builder.param("threads", t);
            builder.threads(Integer.parseInt(t));
        }

        builder.include(QueryStatsDClientAtomicIntegerBenchmarkTest.class.getSimpleName()).forks(2).build();

        new Runner(builder.build()).run();
    }
}
