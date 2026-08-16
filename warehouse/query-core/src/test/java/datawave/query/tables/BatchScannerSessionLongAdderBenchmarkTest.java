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

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 12, time = 12, timeUnit = TimeUnit.SECONDS)

public class BatchScannerSessionLongAdderBenchmarkTest {

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
            instance = new InMemoryInstance(BatchScannerSessionLongAdderBuilder.class.getName());
            client = new InMemoryAccumuloClient("user", instance);

            TableOperations tops = client.tableOperations();

            if (tops.exists(tableName)) {
                tops.delete(tableName);
            }
            tops.create(tableName);

            Long ts = System.currentTimeMillis();
            Key key = new Key("row", "cf", "cq", "VIZ-A", ts);

            try (BatchWriter bw = client.createBatchWriter(tableName)) {
                // Insert 10,000 rows to create actual processing work
                for (int i = 0; i < 10000; i++) {
                    Mutation m = new Mutation("row" + i);
                    m.put("cf", "cq", new Value());
                    bw.addMutation(m);
                }
            }
        }
    }

    @State(Scope.Thread)
    public static class ThreadState {
        public BatchScannerSessionLongAdder scanner;

        @Setup(Level.Trial)
        public void setupThread(GlobalState global) {
            // Build a unique scanner per thread referencing the shared global client
            scanner = BatchScannerSessionLongAdderBuilder.create(global.client).setTableName(global.tableName).setAuthorizations(global.authorizations)
                            .setQuery(global.query).build();
        }
    }

    @Benchmark
    public void benchRun(ThreadState state, Blackhole bh) throws Exception {
        state.scanner.run();

        // Passing the object into the Blackhole prevents Dead Code Elimination (DCE)
        // if scanner.run() returns void or internal state needs to be consumed.
        bh.consume(state.scanner);
    }

    public static void main(String[] args) throws RunnerException {
        String[] threadList = new String[] {"10", "64", "128"};

        OptionsBuilder builder = new OptionsBuilder();

        for (String t : threadList) {
            builder.param("threads", t);
            builder.threads(Integer.parseInt(t));
        }

        builder.include(BatchScannerSessionLongAdderBenchmarkTest.class.getSimpleName()).forks(2).build();

        new Runner(builder.build()).run();
    }
}
