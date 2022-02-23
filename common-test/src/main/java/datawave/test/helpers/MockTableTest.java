package datawave.test.helpers;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import datawave.accumulo.inmemory.InMemoryInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.TimeUnit;

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
        tableOperations.delete(TABLE_NAME);
    }
    
    protected BatchWriter createBatchWriter() throws TableNotFoundException {
        //@formatter:off
        return connector.createBatchWriter(TABLE_NAME, new BatchWriterConfig()
            .setMaxLatency(1L, TimeUnit.SECONDS)
            .setMaxMemory(10000L)
            .setMaxWriteThreads(4)
        );
        //@formatter:on
    }
    
    protected BatchWriter getWriter() {
        return this.writer;
    }
    
    public BatchScanner createBatchScanner(Authorizations authorizations, int threads) throws TableNotFoundException {
        return connector.createBatchScanner(TABLE_NAME, authorizations, threads);
    }
}
