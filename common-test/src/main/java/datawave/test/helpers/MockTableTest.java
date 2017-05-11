package datawave.test.helpers;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.TableOperations;
import datawave.accumulo.inmemory.InMemoryInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.*;

public abstract class MockTableTest {
    public static final String TABLE_NAME = "test";
    protected Connector connector;
    protected BatchWriter writer;
    protected TableOperations tableOperations;
    
    @Before
    public void setup() throws AccumuloSecurityException, AccumuloException, TableNotFoundException, TableExistsException {
        InMemoryInstance i = new InMemoryInstance(this.getClass().toString());
        connector = i.getConnector("root", new PasswordToken(""));
        if (connector.tableOperations().exists(TABLE_NAME))
            connector.tableOperations().delete(TABLE_NAME);
        connector.tableOperations().create(TABLE_NAME);
        writer = createBatchWriter();
        tableOperations = connector.tableOperations();
    }
    
    @After
    public void cleanup() throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        connector.tableOperations().delete(TABLE_NAME);
    }
    
    protected BatchWriter createBatchWriter() throws TableNotFoundException {
        return connector.createBatchWriter(TABLE_NAME, 10000L, 1000L, 4);
    }
    
    public BatchScanner createBatchScanner(Authorizations authorizations, int threads) throws TableNotFoundException {
        return connector.createBatchScanner(TABLE_NAME, authorizations, threads);
    }
}
