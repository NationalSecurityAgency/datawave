package datawave.query.tables;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import datawave.query.tables.async.Scan;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.data.Mutation;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Set;
import java.util.concurrent.TimeUnit;


@ExtendWith(MockitoExtension.class)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@State(Scope.Benchmark)
/**
 * Benchmarks the onSuccess() method from BatchScannerSession after writing data and building the scanner
 */
public class BatchScannerSessionAtomicIntegerBenchmarkTest {
    BatchScannerSession scanner;
    private static final InMemoryInstance instance = new InMemoryInstance(BatchScannerSessionBuilder.class.getName());
    private static AccumuloClient client;
    private static final String tableName = "shard";
    private final Set<Authorizations> authorizations = Set.of(new Authorizations("VIZ-A", "VIZ-B", "VIZ-C"));

    private static final Long ts = System.currentTimeMillis();
    private static final Key key = new Key("row", "cf", "cq", "VIZ-A", ts);
    private static final Value EMPTY_VALUE = new Value();

    private final Query query = new QueryImpl();

    // Reporting label placeholder to allow threads to display in benchmark summary chart
    @Param({"1"})
    public int threads;

    @Setup(Level.Trial)
    public void setUp() throws AccumuloSecurityException, AccumuloException, TableNotFoundException, TableExistsException {
        client = new InMemoryAccumuloClient("user", instance);

        TableOperations tops = client.tableOperations();

        // create or recreate the table
        if (tops.exists(tableName)) {
            tops.delete(tableName);
        }
        tops.create(tableName);

        try (BatchWriter bw = client.createBatchWriter(tableName)) {
            Mutation m = new Mutation(key.getRow());
            m.put(key.getColumnFamily(), key.getColumnQualifier(), key.getColumnVisibilityParsed(), key.getTimestamp(), EMPTY_VALUE);
            bw.addMutation(m);
        }
        BatchScannerSessionBuilder builder = createBuilder();
        scanner = builder.build();
    }

    @Benchmark
    public void benchOnSuccess(){
        Scan mockScan = Mockito.mock(Scan.class);
        scanner.onSuccess(mockScan);
    }

    /**
     * Create a BatchScannerSessionBuilder with the minimum required options
     *
     * @return the builder
     */

    public BatchScannerSessionBuilder createBuilder() {
        //  @formatter:off
        return BatchScannerSessionBuilder.create(client)
                .setTableName(tableName)
                .setAuthorizations(authorizations)
                .setQuery(query);
        //  @formatter:on
    }

    public static void main(String[] args) throws RunnerException {
        String[] threadList = new String[]{"1", "4", "14"};

        OptionsBuilder builder = new OptionsBuilder();

        // Add values to threads param
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
