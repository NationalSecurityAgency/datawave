package datawave.query.statsd;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

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

public class QueryStatsDClientLongAdderBenchmark {

    private QueryStatsDClient qc;

    public QueryStatsDClientLongAdderBenchmark(){

    }

    public QueryStatsDClient getClient(String queryId, String host, int port, int maxCacheSize) {
        if(qc == null){
            qc = new QueryStatsDClient(queryId,host,port,maxCacheSize);
        }
        return qc;
    }

    @Param({"1","10","50","100","300"})
    public int iterations;

    @Setup(Level.Trial)
    public void setUp() {
        getClient("id123","localhost",3000,50);

    }

    @Benchmark
    public void benchNext(){
        for(int i=iterations; i>0; i--){
            qc.next();
        }

    }

    @Benchmark
    public void benchFlush(){
        for(int i=iterations; i>0; i--){
            qc.flush();
        }

    }

    @Benchmark
    public void benchGetSize(){
        for(int i=iterations; i>0; i--){
            qc.getSize();
        }
    }

    public static void main(String[] args) throws RunnerException {
        int[] threadCounts = {1, 2, 4, 6, 8, 10, 12, 14};

        for (int t : threadCounts) {
            System.out.println("===========================================");
            System.out.println("Thread Count: " + t);
            System.out.println("===========================================");
            Options opt = new OptionsBuilder().include(datawave.query.statsd.QueryStatsDClientLongAdderBenchmark.class.getSimpleName())
                    .threads(t).
                    forks(1).build();
            new Runner(opt).run();
        }
    }
}
