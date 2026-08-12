package datawave.query.tables;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
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
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;

@ExtendWith(MockitoExtension.class)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 5, timeUnit = TimeUnit.SECONDS)
public class BatchScannerSessionAtomicIntegerBenchmarkTest {

    // ------------------------------------------------------------------------
    // 1. Shared Global State: Executed ONCE per trial across all threads
    // ------------------------------------------------------------------------
    @State(Scope.Benchmark)
    public static class GlobalState {
        public AccumuloClient client;
        public InMemoryInstance instance;
        public String tableName = "shard";
        public Set<Authorizations> authorizations = Set.of(new Authorizations("VIZ-A", "VIZ-B", "VIZ-C"));
        public Query query = new QueryImpl();

        // Reporting label placeholder to allow threads to display in benchmark summary chart
        @Param({"1"})
        public int threads;

        @Setup(Level.Trial)
        public void setupGlobal() throws AccumuloSecurityException, AccumuloException, TableNotFoundException, TableExistsException {
            instance = new InMemoryInstance(BatchScannerSessionBuilder.class.getName());
            client = new InMemoryAccumuloClient("user", instance);

            TableOperations tops = client.tableOperations();

            // Safely prepare the table once before any thread creates a scanner
            if (tops.exists(tableName)) {
                tops.delete(tableName);
            }
            tops.create(tableName);

            Long ts = System.currentTimeMillis();
            Key key = new Key("row", "cf", "cq", "VIZ-A", ts);

            try (BatchWriter bw = client.createBatchWriter(tableName)) {
                Mutation m = new Mutation(key.getRow());
                m.put(key.getColumnFamily(), key.getColumnQualifier(), key.getColumnVisibilityParsed(), key.getTimestamp(), new Value());
                bw.addMutation(m);
            }
        }
    }

    // ------------------------------------------------------------------------
    // 2. Per-Thread State: Created independently for each thread
    // ------------------------------------------------------------------------
    @State(Scope.Thread)
    public static class ThreadState {
        public BatchScannerSession scanner;

        @Setup(Level.Trial)
        public void setupThread(GlobalState global) {
            // Build a unique scanner per thread referencing the shared global client
            scanner = BatchScannerSessionBuilder.create(global.client)
                    .setTableName(global.tableName)
                    .setAuthorizations(global.authorizations)
                    .setQuery(global.query)
                    .build();
        }
    }

    // ------------------------------------------------------------------------
    // 3. Benchmark Execution
    // ------------------------------------------------------------------------
    @Benchmark
    public void benchRun(ThreadState state, Blackhole bh) throws Exception {
        state.scanner.run();

        // Passing the object into the Blackhole prevents Dead Code Elimination (DCE)
        // if scanner.run() returns void or internal state needs to be consumed.
        bh.consume(state.scanner);
    }

    // ------------------------------------------------------------------------
    // 4. Runner Main Method
    // ------------------------------------------------------------------------
    public static void main(String[] args) throws RunnerException {
        String[] threadList = new String[] {"1", "4", "14"};

        OptionsBuilder builder = new OptionsBuilder();

        for (String t : threadList) {
            builder.param("threads", t);
            builder.threads(Integer.parseInt(t));
        }

        builder.include(BatchScannerSessionAtomicIntegerBenchmarkTest.class.getSimpleName())
                .forks(2)
                .build();

        new Runner(builder.build()).run();
    }
}

/*
# Run complete. Total time: 00:07:36

REMEMBER: The numbers below are just data. To gain reusable insights, you need to follow up on
why the numbers are the way they are. Use profilers (see -prof, -lprof), design factorial
experiments, perform baseline and negative tests that provide experimental control, make sure
the benchmarking environment is safe on JVM/OS/HW level, ask for reviews from the domain experts.
Do not assume the numbers tell you what you want them to tell.

NOTE: Current JVM experimentally supports Compiler Blackholes, and they are in use. Please exercise
extra caution when trusting the results, look into the generated code to check the benchmark still
works, and factor in a small probability of new VM bugs. Additionally, while comparisons between
different JVMs are already problematic, the performance difference caused by different Blackhole
modes can be very significant. Please make sure you use the consistent Blackhole mode for comparisons.

Benchmark                                               (threads)   Mode  Cnt     Score    Error   Units
BatchScannerSessionAtomicIntegerBenchmarkTest.benchRun          1  thrpt   20  1794.183 ± 56.412  ops/us
BatchScannerSessionAtomicIntegerBenchmarkTest.benchRun          4  thrpt   20  1739.311 ±  2.523  ops/us
BatchScannerSessionAtomicIntegerBenchmarkTest.benchRun         14  thrpt   20  1737.679 ±  5.456  ops/us

Process finished with exit code 0

*/
