package datawave.webservice.common.connection;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriterConfig;
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
public class WrappedAccumuloClient implements AccumuloClient {
    private static final Logger log = LoggerFactory.getLogger(WrappedAccumuloClient.class);
    
    private AccumuloClient mock = null;
    private AccumuloClient real = null;
    private String scannerClassLoaderContext = null;
    private long scanBatchTimeoutSeconds = Long.MAX_VALUE;
    private AccumuloClientConfiguration clientConfig = new AccumuloClientConfiguration();
    
    public WrappedAccumuloClient(AccumuloClient real, AccumuloClient mock) {
        this.real = real;
        this.mock = mock;
    }
    
    public void setClientConfig(AccumuloClientConfiguration clientConfig) {
        this.clientConfig = clientConfig;
    }
    
    /**
     * This will update the client configuration with overrides
     * 
     * @param clientConfig
     */
    public void updateClientConfig(AccumuloClientConfiguration clientConfig) {
        AccumuloClientConfiguration merged = new AccumuloClientConfiguration();
        merged.applyOverrides(this.clientConfig);
        merged.applyOverrides(clientConfig);
        this.clientConfig = merged;
    }
    
    @Override
    public BatchScanner createBatchScanner(String tableName, Authorizations authorizations, int numQueryThreads) throws TableNotFoundException {
        return createBatchScanner(tableName, authorizations, numQueryThreads, false);
    }
    
    public BatchScanner createBatchScanner(String tableName, Authorizations authorizations, int numQueryThreads, boolean skipCache)
                    throws TableNotFoundException {
        BatchScannerDelegate delegate;
        if (!skipCache && mock.tableOperations().list().contains(tableName)) {
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
            clientConfig.apply(batchScanner, tableName);
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
    public BatchScanner createBatchScanner(String tableName, Authorizations authorizations) throws TableNotFoundException {
        return createBatchScanner(tableName, authorizations, false);
    }
    
    public BatchScanner createBatchScanner(String tableName, Authorizations authorizations, boolean skipCache) throws TableNotFoundException {
        BatchScannerDelegate delegate;
        if (!skipCache && mock.tableOperations().list().contains(tableName)) {
            if (log.isTraceEnabled()) {
                log.trace("Creating mock batch scanner for table: " + tableName);
            }
            BatchScanner batchScanner = mock.createBatchScanner(tableName, authorizations);
            delegate = new BatchScannerDelegate(batchScanner);
        } else {
            if (log.isTraceEnabled()) {
                log.trace("Creating real batch scanner for table: " + tableName);
            }
            BatchScanner batchScanner = real.createBatchScanner(tableName, authorizations);
            clientConfig.apply(batchScanner, tableName);
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
    public BatchScanner createBatchScanner(String tableName) throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
        return createBatchScanner(tableName, false);
    }
    
    public BatchScanner createBatchScanner(String tableName, boolean skipCache) throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
        BatchScannerDelegate delegate;
        if (!skipCache && mock.tableOperations().list().contains(tableName)) {
            if (log.isTraceEnabled()) {
                log.trace("Creating mock batch scanner for table: " + tableName);
            }
            BatchScanner batchScanner = mock.createBatchScanner(tableName);
            delegate = new BatchScannerDelegate(batchScanner);
        } else {
            if (log.isTraceEnabled()) {
                log.trace("Creating real batch scanner for table: " + tableName);
            }
            BatchScanner batchScanner = real.createBatchScanner(tableName);
            clientConfig.apply(batchScanner, tableName);
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
    public BatchDeleter createBatchDeleter(String tableName, Authorizations authorizations, int numQueryThreads) throws TableNotFoundException {
        return real.createBatchDeleter(tableName, authorizations, numQueryThreads);
    }
    
    @Override
    public BatchWriter createBatchWriter(String tableName) throws TableNotFoundException {
        return real.createBatchWriter(tableName);
    }
    
    @Override
    public MultiTableBatchWriter createMultiTableBatchWriter() {
        return real.createMultiTableBatchWriter();
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
    public ConditionalWriter createConditionalWriter(String tableName) throws TableNotFoundException {
        return real.createConditionalWriter(tableName);
    }
    
    @Override
    public Scanner createScanner(String tableName, Authorizations authorizations) throws TableNotFoundException {
        return createScanner(tableName, authorizations, false);
    }
    
    public Scanner createScanner(String tableName, Authorizations authorizations, boolean skipCache) throws TableNotFoundException {
        ScannerDelegate delegate;
        if (!skipCache && mock.tableOperations().list().contains(tableName)) {
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
            clientConfig.apply(scanner, tableName);
            delegate = new ScannerDelegate(scanner);
            if (scannerClassLoaderContext != null && !"".equals(scannerClassLoaderContext.trim())) {
                log.trace("Setting " + scannerClassLoaderContext + " classpath context on a new scanner.");
                delegate.setContext(scannerClassLoaderContext);
            }
        }
        return delegate;
    }
    
    @Override
    public Scanner createScanner(String tableName) throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
        return createScanner(tableName, false);
    }
    
    public Scanner createScanner(String tableName, boolean skipCache) throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
        Scanner delegate;
        if (!skipCache && mock.tableOperations().list().contains(tableName)) {
            if (log.isTraceEnabled()) {
                log.trace("Creating mock batch scanner for table: " + tableName);
            }
            delegate = mock.createScanner(tableName);
        } else {
            if (log.isTraceEnabled()) {
                log.trace("Creating real batch scanner for table: " + tableName);
            }
            delegate = mock.createScanner(tableName);
            delegate.setBatchTimeout(scanBatchTimeoutSeconds, TimeUnit.SECONDS);
        }
        return delegate;
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
    public Properties properties() {
        return null;
    }
    
    @Override
    public void close() {
        
    }
    
    @Override
    public NamespaceOperations namespaceOperations() {
        return real.namespaceOperations();
    }
    
    public AccumuloClient getMock() {
        return mock;
    }
    
    public AccumuloClient getReal() {
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
