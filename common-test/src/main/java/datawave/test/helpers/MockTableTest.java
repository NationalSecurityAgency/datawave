package datawave.test.helpers;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import datawave.accumulo.inmemory.InMemoryInstance;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.TimeUnit;

public abstract class MockTableTest {
    public static final String TABLE_NAME = "test";
    protected AccumuloClient client;
    protected BatchWriter writer;
    protected TableOperations tableOperations;
    
    @Before
    public void setup() throws AccumuloSecurityException, AccumuloException, TableNotFoundException, TableExistsException {
        InMemoryInstance i = new InMemoryInstance(this.getClass().toString());
        client = new InMemoryAccumuloClient("root", i);
        if (client.tableOperations().exists(TABLE_NAME))
            client.tableOperations().delete(TABLE_NAME);
        client.tableOperations().create(TABLE_NAME);
        writer = createBatchWriter();
        tableOperations = client.tableOperations();
    }
    
    @After
    public void cleanup() throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        client.tableOperations().delete(TABLE_NAME);
    }
    
    protected BatchWriter createBatchWriter() throws TableNotFoundException {
        return client.createBatchWriter(TABLE_NAME,
                        new BatchWriterConfig().setMaxMemory(10000L).setMaxLatency(1000L, TimeUnit.MILLISECONDS).setMaxWriteThreads(4));
    }
    
    protected BatchWriter getWriter() {
        return this.writer;
    }
    
    public BatchScanner createBatchScanner(Authorizations authorizations, int threads) throws TableNotFoundException {
        return client.createBatchScanner(TABLE_NAME, authorizations, threads);
    }
}
