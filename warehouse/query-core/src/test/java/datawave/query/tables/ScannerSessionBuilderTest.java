package datawave.query.tables;

import static org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.util.TableName;

public class ScannerSessionBuilderTest {

    private static final InMemoryInstance instance = new InMemoryInstance(ScannerSessionBuilderTest.class.getSimpleName());
    private static AccumuloClient client;

    private final String tableName = TableName.SHARD;
    private final Set<Authorizations> auths = Collections.singleton(new Authorizations("a", "b", "c"));
    private final ConsistencyLevel level = ConsistencyLevel.IMMEDIATE;

    private ScannerSession session;
    private ScannerSessionBuilder builder;

    @BeforeAll
    public static void setup() throws AccumuloSecurityException, AccumuloException, TableExistsException {
        client = new InMemoryAccumuloClient("user", instance);
        client.tableOperations().create(TableName.SHARD);
    }

    @BeforeEach
    public void beforeEach() {
        session = null;
        builder = null;
    }

    @AfterEach
    public void afterEach() {
        if (session != null) {
            session.close();
        }
    }

    @Test
    public void testBuilderWithNullClient() {
        assertThrows(NullPointerException.class, () -> new ScannerSessionBuilder(null));
    }

    @Test
    public void testBuilderWithNullTableName() {
        builder = new ScannerSessionBuilder(client);
        assertThrows(NullPointerException.class, builder::build);
    }

    @Test
    public void testBuilderWithTableThatDoesNotExist() {
        builder = new ScannerSessionBuilder(client).withTableName("404");
        assertThrows(NullPointerException.class, builder::build);
    }

    @Test
    public void testBuilderWithNullAuths() {
        builder = new ScannerSessionBuilder(client).withTableName(tableName).withAuths(null);
        assertThrows(NullPointerException.class, builder::build);
    }

    @Test
    public void testBuilderWithNullWrapper() {
        builder = new ScannerSessionBuilder(client).withTableName(tableName).withAuths(auths).withWrapper(null);
        assertThrows(NullPointerException.class, builder::build);
    }

    @Test
    public void testBuilderWithMinimalArgs() {
        builder = new ScannerSessionBuilder(client).withTableName(tableName).withAuths(auths).withWrapper(ScannerSession.class);

        assertEquals(tableName, builder.getTableName());
        assertEquals(auths, builder.getAuths());
        assertEquals(ScannerSession.class, builder.getWrapper());
        assertEquals(100, builder.getNumScanResources());
        assertEquals(1_000, builder.getResultQueueSize());
        assertFalse(builder.isStatsEnabled());
        assertNull(builder.getLevel());
        assertNull(builder.getHints());

        session = builder.build();
        assertEquals(level, session.getOptions().getConsistencyLevel());
    }

    @Test
    public void testBuilderWithNumScanResource() {
        builder = new ScannerSessionBuilder(client).withTableName(tableName).withAuths(auths).withWrapper(ScannerSession.class);
        builder.withNumThreads(1);

        assertEquals(1, builder.getNumScanResources());
        session = builder.build();
    }

    @Test
    public void testBuilderWithResultQueueSize() {
        builder = new ScannerSessionBuilder(client).withTableName(tableName).withAuths(auths).withWrapper(ScannerSession.class);
        builder.withResultQueueSize(25);

        assertEquals(25, builder.getResultQueueSize());
        session = builder.build();
    }

    @Test
    public void testBuilderWithStatsEnabled() {
        builder = new ScannerSessionBuilder(client).withTableName(tableName).withAuths(auths).withWrapper(ScannerSession.class);
        builder.withStats(true);

        assertTrue(builder.isStatsEnabled());
        session = builder.build();
    }

    @Test
    public void testBuilderWithConsistencyLevel() {
        builder = new ScannerSessionBuilder(client).withTableName(tableName).withAuths(auths).withWrapper(ScannerSession.class);
        builder.withConsistencyLevel(ConsistencyLevel.EVENTUAL);

        assertEquals(ConsistencyLevel.EVENTUAL, builder.getLevel());
        session = builder.build();
    }

    @Test
    public void testBuilderWithExecutionHints() {
        builder = new ScannerSessionBuilder(client).withTableName(tableName).withAuths(auths).withWrapper(ScannerSession.class);
        builder.withExecutionHints(getHints());

        assertEquals(getHints(), builder.getHints());
        session = builder.build();
    }

    @Test
    public void testBuilderForAnyFieldScanner() {
        builder = new ScannerSessionBuilder(client).withTableName(tableName).withAuths(auths).withWrapper(AnyFieldScanner.class);
        assertEquals(AnyFieldScanner.class, builder.getWrapper());
        session = builder.build();
    }

    @Test
    public void testBuilderForBatchScannerSession() {
        builder = new ScannerSessionBuilder(client).withTableName(tableName).withAuths(auths).withWrapper(BatchScannerSession.class);
        assertEquals(BatchScannerSession.class, builder.getWrapper());
        session = builder.build();
    }

    @Test
    public void testBuilderForRangeStreamScanner() {
        builder = new ScannerSessionBuilder(client).withTableName(tableName).withAuths(auths).withWrapper(RangeStreamScanner.class);
        assertEquals(RangeStreamScanner.class, builder.getWrapper());
        session = builder.build();
    }

    @Test
    public void testFullConfiguration() {
        //  @formatter:off
        builder = new ScannerSessionBuilder(client)
                        .withTableName(tableName)
                        .withAuths(auths)
                        .withWrapper(BatchScannerSession.class)
                        .withNumThreads(25)
                        .withResultQueueSize(150)
                        .withExecutionHints(getHints())
                        .withConsistencyLevel(ConsistencyLevel.EVENTUAL)
                        .withStats(true);
        //  @formatter:on

        assertEquals(tableName, builder.getTableName());
        assertEquals(auths, builder.getAuths());
        assertEquals(BatchScannerSession.class, builder.getWrapper());
        assertEquals(25, builder.getNumScanResources());
        assertEquals(150, builder.getResultQueueSize());
        assertTrue(builder.isStatsEnabled());
        assertEquals(ConsistencyLevel.EVENTUAL, builder.getLevel());
        assertEquals(getHints(), builder.getHints());

        session = builder.build();
    }

    private Map<String,String> getHints() {
        Map<String,String> hints = new HashMap<>();
        hints.put("tableName", "executor-pool");
        return hints;
    }
}
