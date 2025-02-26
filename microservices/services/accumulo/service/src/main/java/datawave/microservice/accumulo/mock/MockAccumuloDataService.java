package datawave.microservice.accumulo.mock;

import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import com.google.common.base.Preconditions;

import datawave.microservice.accumulo.lookup.LookupService;

/**
 * When the <strong>mock</strong> profile is activated, this bean will automatically ingest a tiny amount of data into an in-memory accumulo instance. The
 * tables and data here are designed mainly for this application's unit tests, but the service may also be useful for integration testing
 * <p>
 * TODO: Replace with a better mock data provider (i.e., a more generic service that supports DataWave's query use cases and underlying table schema)
 */
@Service
@Profile("mock")
public class MockAccumuloDataService {
    
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    public static final String WAREHOUSE_MOCK_TABLE = "warehouseTestTable";
    
    private final AccumuloClient warehouseAccumuloClient;
    
    @Autowired
    public MockAccumuloDataService(@Qualifier("warehouse") AccumuloClient warehouseAccumuloClient) {
        this.warehouseAccumuloClient = warehouseAccumuloClient;
        setupMockWarehouseTables();
    }
    
    /**
     * Adds a couple of tables with identical data that are tailored for testing {@link LookupService} and its table-specific audit configs.
     */
    private void setupMockWarehouseTables() {
        try {
            setupMockTable(getWarehouseAccumuloClient(), WAREHOUSE_MOCK_TABLE);
            setupMockTable(getWarehouseAccumuloClient(), WAREHOUSE_MOCK_TABLE + 2);
        } catch (Exception e) {
            log.error("Mock warehouse table setup failed", e);
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Initializes a new table having the specified name with a static dataset for testing. Note that the data here is coupled to audit settings in
     * test/resources/config/application.yml, and coupled to assertions for many unit tests
     *
     * @param accumuloClient
     *            Client to use for accessing Accumulo
     * @param tableName
     *            Accumulo table name
     * @throws Exception
     *             on error
     */
    public void setupMockTable(AccumuloClient accumuloClient, String tableName) throws Exception {
        if (accumuloClient.tableOperations().exists(tableName))
            accumuloClient.tableOperations().delete(tableName);
        
        Preconditions.checkState(!accumuloClient.tableOperations().exists(tableName), tableName + " already exists");
        
        accumuloClient.tableOperations().create(tableName);
        
        BatchWriterConfig bwc = new BatchWriterConfig().setMaxLatency(1l, TimeUnit.SECONDS).setMaxMemory(1024l).setMaxWriteThreads(1);
        try (BatchWriter bw = accumuloClient.createBatchWriter(tableName, bwc)) {
            // Write 3 rows to the test table
            for (int i = 1; i < 4; i++) {
                Mutation m = new Mutation("row" + i);
                m.put("cf1", "cq1", new ColumnVisibility("A&B&C"), 1L, new Value(new byte[0]));
                m.put("cf1", "cq2", new ColumnVisibility("A&D&E"), 1L, new Value(new byte[0]));
                m.put("cf1", "cq3", new ColumnVisibility("A&F&G"), 1L, new Value(new byte[0]));
                m.put("cf2", "cq1", new ColumnVisibility("A"), 1L, new Value(new byte[0]));
                m.put("cf2", "cq2", new ColumnVisibility("B"), 1L, new Value(new byte[0]));
                m.put("cf2", "cq3", new ColumnVisibility("C"), 1L, new Value(new byte[0]));
                m.put("cf2", "cq4", new ColumnVisibility("D"), 1L, new Value(new byte[0]));
                m.put("cf2", "cq5", new ColumnVisibility("E"), 1L, new Value(new byte[0]));
                m.put("cf2", "cq6", new ColumnVisibility("F"), 1L, new Value(new byte[0]));
                m.put("cf2", "cq7", new ColumnVisibility("G"), 1L, new Value(new byte[0]));
                m.put("cf2", "cq8", new ColumnVisibility("H"), 1L, new Value(new byte[0]));
                m.put("cf2", "cq9", new ColumnVisibility("I"), 1L, new Value(new byte[0]));
                bw.addMutation(m);
            }
        }
    }
    
    public AccumuloClient getWarehouseAccumuloClient() {
        return this.warehouseAccumuloClient;
    }
}
