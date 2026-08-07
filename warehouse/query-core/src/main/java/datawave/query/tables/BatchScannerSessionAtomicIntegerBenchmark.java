package datawave.query.tables;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.core.query.configuration.QueryData;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.query.tables.async.Scan;
import datawave.query.tables.async.ScannerChunk;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.openjdk.jmh.annotations.*;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@State(Scope.Benchmark)
/**
 * Benchmarks the onSuccess() method from BatchScannerSession after writing data and building the scanner
 */
public class BatchScannerSessionAtomicIntegerBenchmark {
    BatchScannerSession scanner;
    private static final InMemoryInstance instance = new InMemoryInstance(BatchScannerSessionBuilder.class.getName());
    private static AccumuloClient client;
    private static final String tableName = "shard";
    private final Set<Authorizations> authorizations = Set.of(new Authorizations("VIZ-A", "VIZ-B", "VIZ-C"));

    private static final Long ts = System.currentTimeMillis();
    private static final Key key = new Key("row", "cf", "cq", "VIZ-A", ts);
    private static final Value EMPTY_VALUE = new Value();

    private final Query query = new QueryImpl();

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
    public AtomicInteger benchOnSuccess(){
        // intended usage:
            // scanner.onSuccess(scan);
            // where scan is a Scan object, but I can only make a ScannerChunk currently.
        ScannerChunk chunk = createScannerChunk();

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

    /**
     * Run the scanner
     *
     * @param builder
     *            the builder
     */
    private void buildAndScan(BatchScannerSessionBuilder builder) {
        BatchScannerSession scan = builder.build();
        scan.setChunkIter(List.of(createScannerChunkList()).iterator());
    }

    private List<ScannerChunk> createScannerChunkList() {
        Key start = new Key(key.getRow());
        Key stop = start.followingKey(PartialKey.ROW);
        Range range = new Range(start, true, stop, false);

        IteratorSetting settings = new IteratorSetting(1, "VersioningIterator", VersioningIterator.class);
        QueryData queryData = new QueryData("tableName", "FOO == 'bar'", List.of(range), Collections.emptySet(), List.of(settings));

        ScannerChunk chunk = new ScannerChunk(new SessionOptions(), List.of(range), queryData);
        return List.of(chunk);
    }
    private ScannerChunk createScannerChunk(){
        Key start = new Key(key.getRow());
        Key stop = start.followingKey(PartialKey.ROW);
        Range range = new Range(start, true, stop, false);
        QueryData queryData = new QueryData("tableName", "FOO == 'bar'", List.of(range), Collections.emptySet(), List.of(settings));
        return new ScannerChunk(new SessionOptions(), List.of(range), queryData);
    }

}
