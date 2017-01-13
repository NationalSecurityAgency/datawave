package nsa.datawave.webservice.common.connection;

import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.admin.NamespaceOperations;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;

@SuppressWarnings("deprecation")
public class WrappedConnector extends Connector {
    private final Logger log = Logger.getLogger(WrappedConnector.class);
    
    private Connector mock = null;
    private Connector real = null;
    private String scannerClassLoaderContext = null;
    
    public WrappedConnector(Connector real, Connector mock) {
        this.real = real;
        this.mock = mock;
    }
    
    @Override
    public BatchScanner createBatchScanner(String tableName, Authorizations authorizations, int numQueryThreads) throws TableNotFoundException {
        BatchScanner batchScanner;
        if (mock.tableOperations().list().contains(tableName)) {
            if (log.isTraceEnabled()) {
                log.trace("Creating mock batch scanner for table: " + tableName);
            }
            batchScanner = mock.createBatchScanner(tableName, authorizations, numQueryThreads);
        } else {
            if (log.isTraceEnabled()) {
                log.trace("Creating real batch scanner for table: " + tableName);
            }
            batchScanner = real.createBatchScanner(tableName, authorizations, numQueryThreads);
            if (scannerClassLoaderContext != null && !"".equals(scannerClassLoaderContext.trim())) {
                log.trace("Setting " + scannerClassLoaderContext + " classpath context on a new batch scanner.");
                batchScanner.setContext(scannerClassLoaderContext);
            }
        }
        return new BatchScannerDelegate(batchScanner);
    }
    
    @Override
    public BatchDeleter createBatchDeleter(String tableName, Authorizations authorizations, int numQueryThreads, long maxMemory, long maxLatency,
                    int maxWriteThreads) throws TableNotFoundException {
        return real.createBatchDeleter(tableName, authorizations, numQueryThreads, maxMemory, maxLatency, maxWriteThreads);
    }
    
    @Override
    public BatchWriter createBatchWriter(String tableName, long maxMemory, long maxLatency, int maxWriteThreads) throws TableNotFoundException {
        return real.createBatchWriter(tableName, maxMemory, maxLatency, maxWriteThreads);
    }
    
    @Override
    public MultiTableBatchWriter createMultiTableBatchWriter(long maxMemory, long maxLatency, int maxWriteThreads) {
        return real.createMultiTableBatchWriter(maxMemory, maxLatency, maxWriteThreads);
    }
    
    @Override
    public BatchDeleter createBatchDeleter(String tableName, Authorizations authorizations, int numQueryThreads, BatchWriterConfig config)
                    throws TableNotFoundException {
        BatchDeleter deleter = real.createBatchDeleter(tableName, authorizations, numQueryThreads, config);
        if (scannerClassLoaderContext != null && !"".equals(scannerClassLoaderContext.trim())) {
            log.trace("Setting " + scannerClassLoaderContext + " classpath context on a new batch deleter.");
            deleter.setContext(scannerClassLoaderContext);
        }
        return new BatchDeleterDelegate(deleter);
    }
    
    @Override
    public BatchWriter createBatchWriter(String tableName, BatchWriterConfig config) throws TableNotFoundException {
        return real.createBatchWriter(tableName, config);
    }
    
    @Override
    public MultiTableBatchWriter createMultiTableBatchWriter(BatchWriterConfig config) {
        return real.createMultiTableBatchWriter(config);
    }
    
    @Override
    public ConditionalWriter createConditionalWriter(String tableName, ConditionalWriterConfig config) throws TableNotFoundException {
        return real.createConditionalWriter(tableName, config);
    }
    
    @Override
    public Scanner createScanner(String tableName, Authorizations authorizations) throws TableNotFoundException {
        Scanner scanner;
        if (mock.tableOperations().list().contains(tableName)) {
            if (log.isTraceEnabled()) {
                log.trace("Creating mock scanner for table: " + tableName);
            }
            scanner = mock.createScanner(tableName, authorizations);
        } else {
            if (log.isTraceEnabled()) {
                log.trace("Creating real scanner for table: " + tableName);
            }
            scanner = real.createScanner(tableName, authorizations);
            if (scannerClassLoaderContext != null && !"".equals(scannerClassLoaderContext.trim())) {
                log.trace("Setting " + scannerClassLoaderContext + " classpath context on a new scanner.");
                scanner.setContext(scannerClassLoaderContext);
            }
        }
        return new ScannerDelegate(scanner);
    }
    
    @Override
    public Instance getInstance() {
        return real.getInstance();
    }
    
    @Override
    public String whoami() {
        return real.whoami();
    }
    
    @Override
    public synchronized TableOperations tableOperations() {
        return real.tableOperations();
    }
    
    @Override
    public synchronized SecurityOperations securityOperations() {
        return real.securityOperations();
    }
    
    @Override
    public synchronized InstanceOperations instanceOperations() {
        return real.instanceOperations();
    }
    
    @Override
    public NamespaceOperations namespaceOperations() {
        return real.namespaceOperations();
    }
    
    public Connector getReal() {
        return real;
    }
    
    public void setScannerClassLoaderContext(String scannerClassLoaderContext) {
        this.scannerClassLoaderContext = scannerClassLoaderContext;
    }
    
    public void clearScannerClassLoaderContext() {
        this.scannerClassLoaderContext = null;
    }
}
