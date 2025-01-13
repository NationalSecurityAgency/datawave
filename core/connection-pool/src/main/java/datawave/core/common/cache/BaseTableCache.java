package datawave.core.common.cache;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.NamespaceExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.NamespaceOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.webservice.common.connection.WrappedAccumuloClient;

public class BaseTableCache implements Serializable, TableCache {

    private static final long serialVersionUID = 1L;

    private final transient Logger log = LoggerFactory.getLogger(this.getClass());

    /** should be set by configuration **/
    private String tableName = null;
    private String connectionPoolName = null;
    private String auths = null;
    private long reloadInterval = 0;
    private long maxRows = Long.MAX_VALUE;

    /** set programatically **/
    private Date lastRefresh = new Date(0);
    private AccumuloConnectionFactory connectionFactory = null;
    private transient InMemoryInstance instance = null;
    private SharedCacheCoordinator watcher = null;
    private Future<Boolean> reference = null;

    private ReentrantLock lock = new ReentrantLock();

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public String getConnectionPoolName() {
        return connectionPoolName;
    }

    @Override
    public String getAuths() {
        return auths;
    }

    @Override
    public long getReloadInterval() {
        return reloadInterval;
    }

    @Override
    public Date getLastRefresh() {
        return lastRefresh;
    }

    @Override
    public AccumuloConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    @Override
    public InMemoryInstance getInstance() {
        return instance;
    }

    @Override
    public SharedCacheCoordinator getWatcher() {
        return watcher;
    }

    @Override
    public Future<Boolean> getReference() {
        return reference;
    }

    @Override
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void setConnectionPoolName(String connectionPoolName) {
        this.connectionPoolName = connectionPoolName;
    }

    @Override
    public void setAuths(String auths) {
        this.auths = auths;
    }

    @Override
    public void setReloadInterval(long reloadInterval) {
        this.reloadInterval = reloadInterval;
    }

    @Override
    public void setLastRefresh(Date lastRefresh) {
        this.lastRefresh = lastRefresh;
    }

    @Override
    public void setConnectionFactory(AccumuloConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    @Override
    public void setInstance(InMemoryInstance instance) {
        this.instance = instance;
    }

    @Override
    public void setWatcher(SharedCacheCoordinator watcher) {
        this.watcher = watcher;
    }

    @Override
    public void setReference(Future<Boolean> reference) {
        this.reference = reference;
    }

    @Override
    public long getMaxRows() {
        return this.maxRows;
    }

    @Override
    public void setMaxRows(long maxRows) {
        this.maxRows = maxRows;
    }

    @Override
    public Boolean call() throws Exception {
        if (!lock.tryLock(0, TimeUnit.SECONDS))
            return false;
        // Read from the table in the real Accumulo
        BatchScanner scanner = null;
        BatchWriter writer = null;
        AccumuloClient accumuloClient = null;

        String tempTableName = tableName + "Temp";
        try {
            Map<String,String> map = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            accumuloClient = connectionFactory.getClient(null, null, connectionPoolName, AccumuloConnectionFactory.Priority.ADMIN, map);
            if (accumuloClient instanceof WrappedAccumuloClient) {
                accumuloClient = ((WrappedAccumuloClient) accumuloClient).getReal();
            }
            Authorizations authorizations;
            if (null == auths) {
                authorizations = accumuloClient.securityOperations().getUserAuthorizations(accumuloClient.whoami());
            } else {
                authorizations = new Authorizations(auths);
            }
            scanner = accumuloClient.createBatchScanner(tableName, authorizations, 10);

            AccumuloClient instanceClient = new InMemoryAccumuloClient(AccumuloTableCache.MOCK_USERNAME, instance);
            instanceClient.securityOperations().changeLocalUserPassword(AccumuloTableCache.MOCK_USERNAME, AccumuloTableCache.MOCK_PASSWORD);
            instanceClient.securityOperations().changeUserAuthorizations(AccumuloTableCache.MOCK_USERNAME, authorizations);

            createNamespaceIfNecessary(instanceClient.namespaceOperations(), tempTableName);

            if (instanceClient.tableOperations().exists(tempTableName)) {
                instanceClient.tableOperations().delete(tempTableName);
            }

            instanceClient.tableOperations().create(tempTableName);

            writer = instanceClient.createBatchWriter(tempTableName,
                            new BatchWriterConfig().setMaxMemory(10L * (1024L * 1024L)).setMaxLatency(100L, TimeUnit.MILLISECONDS).setMaxWriteThreads(1));
            setupScanner(scanner);

            Iterator<Entry<Key,Value>> iter = scanner.iterator();
            long count = 0;
            while (iter.hasNext()) {

                if (count > maxRows)
                    break;
                Entry<Key,Value> value = iter.next();

                Key valueKey = value.getKey();

                Mutation m = new Mutation(value.getKey().getRow());
                m.put(valueKey.getColumnFamily(), valueKey.getColumnQualifier(), new ColumnVisibility(valueKey.getColumnVisibility()), valueKey.getTimestamp(),
                                value.getValue());
                writer.addMutation(m);
                count++;
            }
            this.lastRefresh = new Date();
            try {
                instanceClient.tableOperations().delete(tableName);
            } catch (TableNotFoundException e) {
                // the table will not exist the first time this is run
            }
            instanceClient.tableOperations().rename(tempTableName, tableName);
            log.info("Cached {} k,v for table: {}", count, tableName);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        } finally {
            try {
                if (null != accumuloClient)
                    connectionFactory.returnClient(accumuloClient);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
            if (null != scanner)
                scanner.close();
            try {
                if (null != writer)
                    writer.close();
            } catch (Exception e) {
                log.warn("Error closing batch writer for table: {}", tempTableName, e);
            }
            lock.unlock();
        }
        return true;
    }

    public void setupScanner(BatchScanner scanner) {
        scanner.setRanges(Lists.newArrayList(new Range()));
        Map<String,String> options = new HashMap<>();
        options.put(RegExFilter.COLF_REGEX, "^f$");
        options.put("negate", "true");
        IteratorSetting settings = new IteratorSetting(100, "skipFColumn", RegExFilter.class, options);
        scanner.addScanIterator(settings);
    }

    @Override
    public String toString() {
        return "tableName: " + getTableName() + ", connectionPoolName: " + getConnectionPoolName() + ", auths: " + getAuths();
    }

    private void createNamespaceIfNecessary(NamespaceOperations namespaceOperations, String table) throws AccumuloException, AccumuloSecurityException {
        // if the table has a namespace in it that doesn't already exist, create it
        if (table.contains(".")) {
            String namespace = table.split("\\.")[0];
            try {
                if (!namespaceOperations.exists(namespace)) {
                    namespaceOperations.create(namespace);
                }
            } catch (NamespaceExistsException e) {
                // in this case, somebody else must have created the namespace after our existence check
                log.info("Tried to create Accumulo namespace, {}, but it already exists", namespace);
            }
        }
    }
}
