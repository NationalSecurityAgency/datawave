package datawave.microservice.accumulo.mock;

import com.google.common.base.Preconditions;
import datawave.microservice.accumulo.lookup.LookupService;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

/**
 * When the "mock" profile is activated, this service will ingest some data into the configured in-memory accumulo instance, which can be used for testing the
 * accumulo service's functionality.
 * <p>
 * Currently the class simply sets up some tables with static dummy data, which is used during the test phase, but which may also be useful for dev/testing any
 * services that depend on the accumulo service.
 * <p>
 * TODO: Eventually we'll need a mock data service that has broader applicability to the DataWave table schema and its query-specific use cases.
 */
@Service
@Profile("mock")
public class MockAccumuloDataService {
    
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    public static final String WAREHOUSE_MOCK_TABLE = "warehouseTestTable";
    
    private final Connector warehouseConnector;
    
    @Autowired
    public MockAccumuloDataService(@Qualifier("warehouse") Connector warehouseConnector) {
        this.warehouseConnector = warehouseConnector;
        setupMockWarehouseTables();
    }
    
    /**
     * Adds a couple of tables with identical data that are tailored for testing {@link LookupService} and its table-specific audit configs.
     */
    private void setupMockWarehouseTables() {
        try {
            setupMockTable(getWarehouseConnector(), WAREHOUSE_MOCK_TABLE);
            setupMockTable(getWarehouseConnector(), WAREHOUSE_MOCK_TABLE + 2);
        } catch (Throwable t) {
            log.error("Mock warehouse table setup failed", t);
            throw new RuntimeException(t);
        }
    }
    
    /**
     * Initializes a new table having the specified name with a static dataset for testing. Note that the data here is coupled to audit settings in
     * test/resources/config/application.yml, and coupled to assertions for many unit tests
     *
     * @param connector
     *            Accumulo connector
     * @param tableName
     *            Accumulo table name
     * @throws Exception
     *             on error
     */
    public void setupMockTable(Connector connector, String tableName) throws Exception {
        if (connector.tableOperations().exists(tableName))
            connector.tableOperations().delete(tableName);
        
        Preconditions.checkState(!connector.tableOperations().exists(tableName), tableName + " already exists");
        
        connector.tableOperations().create(tableName);
        
        Preconditions.checkState(connector.tableOperations().exists(tableName), tableName + " doesn't exist");
        
        BatchWriterConfig bwc = new BatchWriterConfig().setMaxLatency(1l, TimeUnit.SECONDS).setMaxMemory(1024l).setMaxWriteThreads(1);
        try (BatchWriter bw = connector.createBatchWriter(tableName, bwc)) {
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
    
    public Connector getWarehouseConnector() {
        return this.warehouseConnector;
    }
}
