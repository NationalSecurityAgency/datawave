package datawave.query.tables;

import static org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.util.TableName;

public class BatchScannerBuilderTest {

    private static final InMemoryInstance instance = new InMemoryInstance(BatchScannerBuilderTest.class.getSimpleName());
    private static AccumuloClient client;

    private final String tableName = TableName.SHARD;
    private final Set<Authorizations> auths = Collections.singleton(new Authorizations("a", "b", "c"));
    private final ConsistencyLevel level = ConsistencyLevel.IMMEDIATE;

    private BatchScanner scanner;
    private BatchScannerBuilder builder;

    @BeforeAll
    public static void setup() throws AccumuloSecurityException, AccumuloException, TableExistsException {
        client = new InMemoryAccumuloClient("user", instance);
        client.tableOperations().create(TableName.SHARD);
    }

    @BeforeEach
    public void beforeEach() {
        scanner = null;
        builder = null;
    }

    @AfterEach
    public void afterEach() {
        if (scanner != null) {
            scanner.close();
        }
    }

    @Test
    public void testBuilderWithNullClient() {
        assertThrows(NullPointerException.class, () -> new BatchScannerBuilder(null));
    }

    @Test
    public void testBuilderWithNullTableName() {
        builder = new BatchScannerBuilder(client).withTableName(null);
        assertThrows(NullPointerException.class, builder::build);
    }

    @Test
    public void testBuilderForTableThatDoesNotExist() {
        builder = new BatchScannerBuilder(client).withTableName("whereiswaldo");
        assertThrows(RuntimeException.class, builder::build);
    }

    @Test
    public void testBuilderWithNullAuths() {
        builder = new BatchScannerBuilder(client).withTableName(tableName).withAuths(null);
        assertThrows(NullPointerException.class, builder::build);
    }

    @Test
    public void testBuilderWithMinimalArgs() {
        builder = new BatchScannerBuilder(client).withTableName(tableName).withAuths(auths);
        assertEquals(tableName, builder.getTableName());
        assertEquals(auths, builder.getAuths());
        assertEquals(8, builder.getNumThreads());
        assertNull(builder.getQuery());
        assertNull(builder.getLevel());
        assertNull(builder.getHints());

        scanner = builder.build();
        assertEquals(level, scanner.getConsistencyLevel());
    }

    @Test
    public void testBuilderWithQuery() {
        builder = new BatchScannerBuilder(client).withTableName(tableName).withAuths(auths).withQuery(getQuery());

        assertEquals(getQuery(), builder.getQuery());

        scanner = builder.build();
    }

    @Test
    public void testBuilderWithConsistencyLevel() {
        builder = new BatchScannerBuilder(client).withTableName(tableName).withAuths(auths).withConsistencyLevel(ConsistencyLevel.EVENTUAL);

        assertEquals(ConsistencyLevel.EVENTUAL, builder.getLevel());

        scanner = builder.build();
    }

    @Test
    public void testBuilderWithExecutionHints() {
        builder = new BatchScannerBuilder(client).withTableName(tableName).withAuths(auths).withExecutionHints(getHints());

        assertEquals(getHints(), builder.getHints());

        scanner = builder.build();
    }

    @Test
    public void testFullConfiguration() {
        //  @formatter:off
        builder = new BatchScannerBuilder(client)
                        .withTableName(tableName)
                        .withAuths(auths)
                        .withNumThreads(8)
                        .withConsistencyLevel(ConsistencyLevel.EVENTUAL)
                        .withExecutionHints(getHints());
        //  @formatter:on

        assertEquals(tableName, builder.getTableName());
        assertEquals(auths, builder.getAuths());
        assertEquals(8, builder.getNumThreads());
        assertEquals(ConsistencyLevel.EVENTUAL, builder.getLevel());
        assertEquals(getHints(), builder.getHints());

        scanner = builder.build();
    }

    private Query getQuery() {
        QueryImpl query = new QueryImpl();
        query.setQuery("FOO == 'bar'");
        return query;
    }

    private Map<String,String> getHints() {
        Map<String,String> hints = new HashMap<>();
        hints.put("tableName", "executor-pool");
        return hints;
    }
}
