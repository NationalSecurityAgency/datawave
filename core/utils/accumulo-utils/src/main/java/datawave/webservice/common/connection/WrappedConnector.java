package datawave.webservice.common.connection;

import java.util.concurrent.TimeUnit;

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
import org.apache.accumulo.core.client.admin.ReplicationOperations;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.security.Authorizations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("deprecation")
public class WrappedConnector extends Connector {
    private static final Logger log = LoggerFactory.getLogger(WrappedConnector.class);
    
    private Connector mock = null;
    private Connector real = null;
    private String scannerClassLoaderContext = null;
    private long scanBatchTimeoutSeconds = Long.MAX_VALUE;
    
    public WrappedConnector(Connector real, Connector mock) {
        this.real = real;
        this.mock = mock;
    }
    
    @Override
    public BatchScanner createBatchScanner(String tableName, Authorizations authorizations, int numQueryThreads) throws TableNotFoundException {
        BatchScannerDelegate delegate;
        if (mock.tableOperations().list().contains(tableName)) {
            if (log.isTraceEnabled()) {
                log.trace("Creating mock batch scanner for table: " + tableName);
            }
            BatchScanner batchScanner = mock.createBatchScanner(tableName, authorizations, numQueryThreads);
            delegate = new BatchScannerDelegate(batchScanner);
        } else {
            if (log.isTraceEnabled()) {
                log.trace("Creating real batch scanner for table: " + tableName);
            }
            BatchScanner batchScanner = real.createBatchScanner(tableName, authorizations, numQueryThreads);
            delegate = new BatchScannerDelegate(batchScanner);
            if (scannerClassLoaderContext != null && !"".equals(scannerClassLoaderContext.trim())) {
                log.trace("Setting " + scannerClassLoaderContext + " classpath context on a new batch scanner.");
                delegate.setContext(scannerClassLoaderContext);
            }
            delegate.setBatchTimeout(scanBatchTimeoutSeconds, TimeUnit.SECONDS);
        }
        return delegate;
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
        BatchDeleterDelegate delegate = new BatchDeleterDelegate(deleter);
        if (scannerClassLoaderContext != null && !"".equals(scannerClassLoaderContext.trim())) {
            log.trace("Setting " + scannerClassLoaderContext + " classpath context on a new batch deleter.");
            delegate.setContext(scannerClassLoaderContext);
        }
        return delegate;
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
        ScannerDelegate delegate;
        if (mock.tableOperations().list().contains(tableName)) {
            if (log.isTraceEnabled()) {
                log.trace("Creating mock scanner for table: " + tableName);
            }
            Scanner scanner = mock.createScanner(tableName, authorizations);
            delegate = new ScannerDelegate(scanner);
        } else {
            if (log.isTraceEnabled()) {
                log.trace("Creating real scanner for table: " + tableName);
            }
            Scanner scanner = real.createScanner(tableName, authorizations);
            delegate = new ScannerDelegate(scanner);
            if (scannerClassLoaderContext != null && !"".equals(scannerClassLoaderContext.trim())) {
                log.trace("Setting " + scannerClassLoaderContext + " classpath context on a new scanner.");
                delegate.setContext(scannerClassLoaderContext);
            }
        }
        return delegate;
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
    public ReplicationOperations replicationOperations() {
        return real.replicationOperations();
    }
    
    @Override
    public NamespaceOperations namespaceOperations() {
        return real.namespaceOperations();
    }
    
    public Connector getMock() {
        return mock;
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
    
    public long getScanBatchTimeoutSeconds() {
        return scanBatchTimeoutSeconds;
    }
    
    public void setScanBatchTimeoutSeconds(long scanBatchTimeoutSeconds) {
        this.scanBatchTimeoutSeconds = scanBatchTimeoutSeconds;
    }
    
}
