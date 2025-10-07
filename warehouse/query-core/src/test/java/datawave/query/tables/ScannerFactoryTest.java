package datawave.query.tables;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.query.config.ShardQueryConfiguration;
import datawave.util.TableName;

class ScannerFactoryTest {

    private static final InMemoryInstance instance = new InMemoryInstance(ScannerFactoryTest.class.getSimpleName());
    private static ScannerFactory scannerFactory;
    private static final ShardQueryConfiguration config = new ShardQueryConfiguration();

    private static final String ALT_INDEX = "altIndex";

    @BeforeAll
    public static void before() throws Exception {
        AccumuloClient client = new MyAccumuloClient("", instance);
        config.setClient(client);
        scannerFactory = new ScannerFactory(config);

        client.tableOperations().create(TableName.SHARD);
        client.tableOperations().create(TableName.SHARD_INDEX);
        client.tableOperations().create(ALT_INDEX);
        client.instanceOperations().setProperty("accumulo.instance.name", "required-for-tests");
    }

    @BeforeEach
    public void setup() {
        Map<String,ScannerBase.ConsistencyLevel> consistencyLevels = new HashMap<>();
        consistencyLevels.put(TableName.SHARD, ScannerBase.ConsistencyLevel.IMMEDIATE);
        config.setTableConsistencyLevels(consistencyLevels);
        scannerFactory.updateConfigs(config);
    }

    @Test
    void testSingleScannerWithTableNameAuthsQuery() throws TableNotFoundException {
        Scanner scanner = scannerFactory.newSingleScanner(TableName.SHARD, getAuths(), getQuery());
        assertImmediateConsistency(scanner);

        setEventualConsistency();
        scanner = scannerFactory.newSingleScanner(TableName.SHARD, getAuths(), getQuery());
        assertEventualConsistency(scanner);
    }

    @Test
    void testSingleScannerWithTableNameAuthsThreadsQuery() throws TableNotFoundException {
        BatchScanner scanner = scannerFactory.newScanner(TableName.SHARD, getAuths(), 1, getQuery());
        assertImmediateConsistency(scanner);

        setEventualConsistency();
        scanner = scannerFactory.newScanner(TableName.SHARD, getAuths(), 1, getQuery());
        assertEventualConsistency(scanner);
    }

    @Test
    void testSingleScannerWithTableNameAuthsThreadsQueryReportErrors() throws TableNotFoundException {
        BatchScanner scanner = scannerFactory.newScanner(TableName.SHARD, getAuths(), 1, getQuery(), true);
        assertImmediateConsistency(scanner);

        setEventualConsistency();
        scanner = scannerFactory.newScanner(TableName.SHARD, getAuths(), 1, getQuery(), true);
        assertEventualConsistency(scanner);
    }

    @Test
    void testScannerWithAuthsQuery() throws TableNotFoundException {
        BatchScanner scanner = scannerFactory.newScanner(TableName.SHARD, getAuths(), getQuery());
        assertImmediateConsistency(scanner);

        setEventualConsistency();
        scanner = scannerFactory.newScanner(TableName.SHARD, getAuths(), getQuery());
        assertEventualConsistency(scanner);
    }

    @Test
    void testScannerWithQuery() throws TableNotFoundException {
        BatchScanner scanner = scannerFactory.newScanner(TableName.SHARD, getQuery());
        assertImmediateConsistency(scanner);

        setEventualConsistency();
        scanner = scannerFactory.newScanner(TableName.SHARD, getQuery());
        assertEventualConsistency(scanner);
    }

    @Test
    void testQueryScannerWithAuthsQuery() throws Exception {
        BatchScannerSession scanner = scannerFactory.newQueryScanner(TableName.SHARD, getAuths(), getQuery());
        assertImmediateConsistency(scanner);

        setEventualConsistency();
        scanner = scannerFactory.newQueryScanner(TableName.SHARD, getAuths(), getQuery());
        assertEventualConsistency(scanner);
    }

    @Test
    void testLimitedScannerAsAnyFieldScanner() throws Exception {
        AnyFieldScanner scanner = scannerFactory.newLimitedScanner(AnyFieldScanner.class, TableName.SHARD, getAuths(), getQuery());
        assertImmediateConsistency(scanner);

        setEventualConsistency();
        scanner = scannerFactory.newLimitedScanner(AnyFieldScanner.class, TableName.SHARD, getAuths(), getQuery());
        assertEventualConsistency(scanner);
    }

    @Test
    void testLimitedScannerAsRangeStreamScanner() throws Exception {
        RangeStreamScanner scanner = scannerFactory.newLimitedScanner(RangeStreamScanner.class, TableName.SHARD, getAuths(), getQuery());
        assertImmediateConsistency(scanner);

        setEventualConsistency();
        scanner = scannerFactory.newLimitedScanner(RangeStreamScanner.class, TableName.SHARD, getAuths(), getQuery());
        assertEventualConsistency(scanner);
    }

    @Test
    void testLimitedScannerAsBatchScannerSession() throws Exception {
        BatchScannerSession scanner = scannerFactory.newLimitedScanner(BatchScannerSession.class, TableName.SHARD, getAuths(), getQuery());
        assertImmediateConsistency(scanner);

        setEventualConsistency();
        scanner = scannerFactory.newLimitedScanner(BatchScannerSession.class, TableName.SHARD, getAuths(), getQuery());
        assertEventualConsistency(scanner);
    }

    @Test
    void testRangeScannerWithAuthsQuery() throws Exception {
        RangeStreamScanner scanner = scannerFactory.newRangeScanner(TableName.SHARD, getAuths(), getQuery());
        assertImmediateConsistency(scanner);

        setEventualConsistency();
        scanner = scannerFactory.newRangeScanner(TableName.SHARD, getAuths(), getQuery());
        assertEventualConsistency(scanner);
    }

    @Test
    void testRangeScannerWithAuthsQueryThreshold() throws Exception {
        RangeStreamScanner scanner = scannerFactory.newRangeScanner(TableName.SHARD, getAuths(), getQuery(), 123);
        assertImmediateConsistency(scanner);

        setEventualConsistency();
        scanner = scannerFactory.newRangeScanner(TableName.SHARD, getAuths(), getQuery(), 123);
        assertEventualConsistency(scanner);
    }

    @Test
    void testRFileScanner() {
        ScannerBase scanner = scannerFactory.newRfileScanner(TableName.SHARD, getAuths(), getQuery());
        assertImmediateConsistency(scanner);

        setEventualConsistency();
        scanner = scannerFactory.newRfileScanner(TableName.SHARD, getAuths(), getQuery());
        assertEventualConsistency(scanner);
    }

    @Test
    public void testSingleScannerWithAbsentTableName() throws Exception {
        Scanner scanner = scannerFactory.newSingleScanner(ALT_INDEX, getAuths(), getQuery());
        assertImmediateConsistency(scanner);
    }

    @Test
    public void testScannerWithAbsentTableName() throws Exception {
        BatchScanner scanner = scannerFactory.newScanner(ALT_INDEX, getQuery());
        assertImmediateConsistency(scanner);

        scanner = scannerFactory.newScanner(ALT_INDEX, getAuths(), 1, getQuery(), "ALT_HINT");
        assertImmediateConsistency(scanner);

        scanner = scannerFactory.newScanner(ALT_INDEX, getAuths(), 1, getQuery(), null);
        assertImmediateConsistency(scanner);
    }

    @Test
    public void testQueryScannerWithAbsentTableName() throws Exception {
        BatchScannerSession scanner = scannerFactory.newQueryScanner(ALT_INDEX, getAuths(), getQuery());
        assertImmediateConsistency(scanner);

        scanner = scannerFactory.newQueryScanner(ALT_INDEX, getAuths(), getQuery(), "ALT_HINT");
        assertImmediateConsistency(scanner);

        scanner = scannerFactory.newQueryScanner(ALT_INDEX, getAuths(), getQuery(), null);
        assertImmediateConsistency(scanner);
    }

    @Test
    public void testLimitedAnyFieldScannerWithAbsentTableName() throws Exception {
        AnyFieldScanner scanner = scannerFactory.newLimitedScanner(AnyFieldScanner.class, ALT_INDEX, getAuths(), getQuery());
        assertImmediateConsistency(scanner);

        scanner = scannerFactory.newLimitedScanner(AnyFieldScanner.class, ALT_INDEX, getAuths(), getQuery(), "ALT_HINT");
        assertImmediateConsistency(scanner);

        scanner = scannerFactory.newLimitedScanner(AnyFieldScanner.class, ALT_INDEX, getAuths(), getQuery(), null);
        assertImmediateConsistency(scanner);
    }

    @Test
    public void testLimitedRangeStreamScannerWithAbsentTableName() throws Exception {
        RangeStreamScanner scanner = scannerFactory.newLimitedScanner(RangeStreamScanner.class, ALT_INDEX, getAuths(), getQuery());
        assertImmediateConsistency(scanner);

        scanner = scannerFactory.newLimitedScanner(RangeStreamScanner.class, ALT_INDEX, getAuths(), getQuery(), "ALT_HINT");
        assertImmediateConsistency(scanner);

        scanner = scannerFactory.newLimitedScanner(RangeStreamScanner.class, ALT_INDEX, getAuths(), getQuery(), null);
        assertImmediateConsistency(scanner);
    }

    @Test
    public void testLimitedBatchScannerSessionWithAbsentTableName() throws Exception {
        BatchScannerSession scanner = scannerFactory.newLimitedScanner(BatchScannerSession.class, ALT_INDEX, getAuths(), getQuery());
        assertImmediateConsistency(scanner);

        scanner = scannerFactory.newLimitedScanner(BatchScannerSession.class, ALT_INDEX, getAuths(), getQuery(), "ALT_HINT");
        assertImmediateConsistency(scanner);

        scanner = scannerFactory.newLimitedScanner(BatchScannerSession.class, ALT_INDEX, getAuths(), getQuery(), null);
        assertImmediateConsistency(scanner);
    }

    private void setEventualConsistency() {
        Map<String,ScannerBase.ConsistencyLevel> consistencyLevels = new HashMap<>();
        consistencyLevels.put(TableName.SHARD, ScannerBase.ConsistencyLevel.EVENTUAL);
        config.setTableConsistencyLevels(consistencyLevels);
        scannerFactory.updateConfigs(config);
    }

    private void assertEventualConsistency(Scanner scanner) {
        assertEquals(ScannerBase.ConsistencyLevel.EVENTUAL, scanner.getConsistencyLevel());
    }

    private void assertEventualConsistency(ScannerBase scanner) {
        assertEquals(ScannerBase.ConsistencyLevel.EVENTUAL, scanner.getConsistencyLevel());
    }

    private void assertEventualConsistency(ScannerSession session) {
        assertEquals(ScannerBase.ConsistencyLevel.EVENTUAL, session.getOptions().getConsistencyLevel());
    }

    private void assertImmediateConsistency(Scanner scanner) {
        assertEquals(ScannerBase.ConsistencyLevel.IMMEDIATE, scanner.getConsistencyLevel());
    }

    private void assertImmediateConsistency(ScannerBase scanner) {
        assertEquals(ScannerBase.ConsistencyLevel.IMMEDIATE, scanner.getConsistencyLevel());
    }

    private void assertImmediateConsistency(ScannerSession session) {
        assertEquals(ScannerBase.ConsistencyLevel.IMMEDIATE, session.getOptions().getConsistencyLevel());
    }

    private Query getQuery() {
        return new QueryImpl();
    }

    private Set<Authorizations> getAuths() {
        return Collections.singleton(new Authorizations());
    }

    /**
     * The RFileScanner factory method requires two client properties to be set. The InMemoryAccumuloClient does not support setting these properties, or any
     * properties so to fully test the ScannerFactory we must override the {@link InMemoryAccumuloClient#properties()} method.
     */
    static class MyAccumuloClient extends InMemoryAccumuloClient {

        public MyAccumuloClient(String username, InMemoryInstance instance) throws AccumuloSecurityException {
            super(username, instance);
        }

        @Override
        public Properties properties() {
            Properties props = new Properties();
            props.put(ClientProperty.INSTANCE_NAME.getKey(), "for-testing");
            props.put(ClientProperty.INSTANCE_ZOOKEEPERS.getKey(), "for-testing");
            return props;
        }
    }

}
