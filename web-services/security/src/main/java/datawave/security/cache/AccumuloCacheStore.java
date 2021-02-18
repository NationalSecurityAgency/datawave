package datawave.security.cache;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.user.AgeOffFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.commons.io.ByteBufferFactory;
import org.infinispan.filter.KeyFilter;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.TaskContextImpl;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.PersistenceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ConfiguredBy(AccumuloCacheStoreConfiguration.class)
public class AccumuloCacheStore<K extends Serializable,V> implements AdvancedLoadWriteStore<K,V> {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private InitializationContext ctx;
    private AccumuloCacheStoreConfiguration configuration;
    private String tableName;
    private Authorizations authorizations = new Authorizations();
    
    private AccumuloClient accumuloClient;
    private BatchWriter batchWriter;
    
    @Override
    public void init(InitializationContext ctx) {
        this.ctx = ctx;
        configuration = ctx.getConfiguration();
        tableName = configuration.tableName();
        List<String> auths = configuration.auths();
        if (auths != null && !auths.isEmpty())
            authorizations = new Authorizations(auths.toArray(new String[0]));
    }
    
    @Override
    public void start() {
        // @formatter:off
        accumuloClient = Accumulo.newClient()
                .to(configuration.instanceName(), configuration.zookeepers())
                .as(configuration.username(), configuration.password())
                .build();
        // @formatter:on
        
        IteratorSetting ageoffConfig = new IteratorSetting(configuration.ageoffPriority(), AgeOffFilter.class.getSimpleName(), AgeOffFilter.class);
        AgeOffFilter.setTTL(ageoffConfig, configuration.ageoffTTLhours() * 60L * 60L * 1000L);
        
        if (!accumuloClient.tableOperations().exists(tableName)) {
            try {
                accumuloClient.tableOperations().create(tableName);
                accumuloClient.tableOperations().attachIterator(tableName, ageoffConfig, EnumSet.allOf(IteratorUtil.IteratorScope.class));
            } catch (TableExistsException e) {
                log.debug("Attempted to create cache table {} but someone else beat us to the punch.", tableName);
            } catch (TableNotFoundException e) {
                // ignore -- we just created the table so it must exist
                log.error("The impossible happened - created a table and set an iterator on it but the table doesn't exist.", e);
            } catch (AccumuloException | AccumuloSecurityException e) {
                throw new RuntimeException("Unable to create Accumulo cache table " + tableName, e);
            }
        } else {
            // Update ageoff iterator setting in case it changed: remove and re-add since there's no update option on the API
            // (we could do it by setting the underlying configuration property, but that seems fragile)
            // We do need to handle the potential race condition where multiple servers are starting at the same time and
            // one server might remove the setting, a second server might remove and re-add, and then the first server tries
            // to add again, causing an iterator name conflict.
            try {
                IteratorSetting existingSetting = accumuloClient.tableOperations().getIteratorSetting(tableName, ageoffConfig.getName(),
                                IteratorUtil.IteratorScope.scan);
                if (existingSetting == null || !existingSetting.equals(ageoffConfig)) {
                    accumuloClient.tableOperations().removeIterator(tableName, AgeOffFilter.class.getSimpleName(),
                                    EnumSet.allOf(IteratorUtil.IteratorScope.class));
                    try {
                        accumuloClient.tableOperations().attachIterator(tableName, ageoffConfig, EnumSet.allOf(IteratorUtil.IteratorScope.class));
                    } catch (IllegalArgumentException e) {
                        log.trace("Hit race condition on configuring age-off iterator. Ignoring exception.");
                    }
                }
            } catch (TableNotFoundException e) {
                log.error("Table exists returned true, but operations on it reported TableNotFound.", e);
            } catch (AccumuloException | AccumuloSecurityException e) {
                throw new RuntimeException("Unable to configure Accumulo cache table " + tableName, e);
            }
        }
        
        BatchWriterConfig bwConfig = new BatchWriterConfig().setMaxWriteThreads(configuration.writeThreads())
                        .setMaxLatency(configuration.maxLatency(), TimeUnit.SECONDS).setMaxMemory(configuration.maxMemory());
        try {
            batchWriter = accumuloClient.createBatchWriter(tableName, bwConfig);
        } catch (TableNotFoundException e) {
            // should never happen - we create the table right here
            throw new RuntimeException("Unable to create BatchWriter.", e);
        }
    }
    
    @Override
    public void stop() {
        try {
            batchWriter.close();
        } catch (MutationsRejectedException e) {
            throw new RuntimeException("Unable to write cache value(s) to Accumulo", e);
        }
        
        accumuloClient.close();
    }
    
    @Override
    public void write(MarshalledEntry<? extends K,? extends V> entry) {
        log.trace("Adding value for {} to the accumulo cache for table {}.", entry.getKey(), tableName);
        
        org.infinispan.commons.io.ByteBuffer keyBytes = entry.getKeyBytes();
        org.infinispan.commons.io.ByteBuffer valueBytes = entry.getValueBytes();
        org.infinispan.commons.io.ByteBuffer metadataBytes = entry.getMetadataBytes();
        int metadataLength = metadataBytes == null ? 0 : metadataBytes.getLength();
        int len = 4 + 4 + valueBytes.getLength() + metadataLength;
        
        ByteBuffer buffer = ByteBuffer.allocate(len);
        buffer.putInt(valueBytes.getLength());
        buffer.putInt(metadataLength);
        buffer.put(valueBytes.getBuf(), valueBytes.getOffset(), valueBytes.getLength());
        if (metadataBytes != null)
            buffer.put(metadataBytes.getBuf(), metadataBytes.getOffset(), metadataBytes.getLength());
        buffer.flip();
        
        Mutation m = new Mutation(keyBytes.getBuf(), keyBytes.getOffset(), keyBytes.getLength());
        m.put("", "", entry.getMetadata().created(), new Value(buffer));
        try {
            batchWriter.addMutation(m);
            batchWriter.flush();
        } catch (MutationsRejectedException e) {
            throw new PersistenceException("Unable to write cache value to Accumulo", e);
        }
    }
    
    @Override
    public void clear() {
        log.trace("Clearing Accumulo cache for table {}.", tableName);
        try {
            BatchWriterConfig bwCfg = new BatchWriterConfig();
            try (BatchDeleter deleter = accumuloClient.createBatchDeleter(tableName, authorizations, 10, bwCfg)) {
                deleter.setRanges(Collections.singletonList(new Range()));
                deleter.delete();
            }
        } catch (MutationsRejectedException | TableNotFoundException e) {
            throw new PersistenceException("Unable to clear Accumulo cache for " + tableName, e);
        }
    }
    
    @Override
    public boolean delete(Object key) {
        log.trace("Deleting value for {} from the Accumulo cache for table {}.", key, tableName);
        try {
            byte[] keyBytes = ctx.getMarshaller().objectToByteBuffer(key);
            Mutation m = new Mutation(keyBytes);
            m.putDelete("", "");
            try {
                batchWriter.addMutation(m);
                batchWriter.flush();
                return true;
            } catch (MutationsRejectedException e) {
                throw new PersistenceException("Unable to write cache value to Accumulo", e);
            }
        } catch (IOException | InterruptedException e) {
            throw new PersistenceException("Unable to serialize key: " + key, e);
        }
    }
    
    @Override
    public MarshalledEntry<K,V> load(Object key) {
        return _load(key, true, true);
    }
    
    public MarshalledEntry<K,V> _load(Object key, boolean loadValue, boolean loadMetadata) {
        Scanner scanner;
        try {
            scanner = accumuloClient.createScanner(tableName, authorizations);
            byte[] keyBytes = ctx.getMarshaller().objectToByteBuffer(key);
            scanner.setRange(new Range(new Text(keyBytes)));
        } catch (TableNotFoundException e) {
            throw new PersistenceException(e);
        } catch (IOException | InterruptedException e) {
            throw new PersistenceException("Unable to serialize key " + key, e);
        }
        
        Iterator<Map.Entry<Key,Value>> iterator = scanner.iterator();
        Map.Entry<Key,Value> entry = iterator.hasNext() ? iterator.next() : null;
        return decodeEntry(entry, key, loadValue, loadMetadata);
    }
    
    private MarshalledEntry<K,V> decodeEntry(Map.Entry<Key,Value> entry, Object key, boolean loadValue, boolean loadMetadata) {
        if (entry != null) {
            ByteBufferFactory bbFactory = ctx.getByteBufferFactory();
            ByteBuffer buffer = ByteBuffer.wrap(entry.getValue().get(), 0, entry.getValue().getSize());
            int valueSize = buffer.getInt();
            int metadataSize = buffer.getInt();
            org.infinispan.commons.io.ByteBuffer valueBytes = null;
            if (loadValue) {
                valueBytes = bbFactory.newByteBuffer(entry.getValue().get(), buffer.position(), valueSize);
            }
            org.infinispan.commons.io.ByteBuffer metadataBytes = null;
            if (metadataSize > 0 && loadMetadata) {
                // Skip over value if we didn't read it in.
                if (!loadValue)
                    buffer.position(buffer.position() + valueSize);
                metadataBytes = bbFactory.newByteBuffer(entry.getValue().get(), buffer.position() + valueSize, metadataSize);
            }
            // noinspection unchecked
            return ctx.getMarshalledEntryFactory().newMarshalledEntry(key, valueBytes, metadataBytes);
        } else {
            return null;
        }
    }
    
    @Override
    public boolean contains(Object key) {
        try (Scanner scanner = accumuloClient.createScanner(tableName, authorizations)) {
            scanner.setRange(new Range(String.valueOf(key)));
            Iterator<Map.Entry<Key,Value>> iterator = scanner.iterator();
            return iterator.hasNext();
        } catch (TableNotFoundException e) {
            throw new PersistenceException(e);
        }
    }
    
    @Override
    public int size() {
        try (BatchScanner batchScanner = accumuloClient.createBatchScanner(tableName, authorizations, 5)) {
            batchScanner.setRanges(Collections.singleton(new Range()));
            try {
                int rows = 0;
                for (Iterator<Map.Entry<Key,Value>> it = batchScanner.iterator(); it.hasNext(); it.next()) {
                    rows++;
                }
                return rows;
            } finally {
                batchScanner.close();
            }
        } catch (TableNotFoundException e) {
            throw new PersistenceException("Unable to calculate size of Accumulo cache table " + tableName, e);
        }
    }
    
    @Override
    public void purge(Executor threadPool, PurgeListener<? super K> listener) {
        // This is a no-op since we use an age-off iterator on the cache entries
    }
    
    @Override
    public void process(KeyFilter<? super K> filter, CacheLoaderTask<K,V> task, Executor executor, boolean fetchValue, boolean fetchMetadata) {
        try (BatchScanner batchScanner = accumuloClient.createBatchScanner(tableName, authorizations, 5)) {
            
            batchScanner.setRanges(Collections.singleton(new Range()));
            try {
                TaskContext taskContext = new TaskContextImpl();
                for (Map.Entry<Key,Value> entry : batchScanner) {
                    if (taskContext.isStopped())
                        break;
                    
                    try {
                        ByteSequence rowData = entry.getKey().getRowData();
                        @SuppressWarnings("unchecked")
                        K key = (K) ctx.getMarshaller().objectFromByteBuffer(rowData.getBackingArray(), rowData.offset(), rowData.length());
                        if (filter.accept(key)) {
                            MarshalledEntry<K,V> marshalledEntry = decodeEntry(entry, key, fetchValue, fetchMetadata);
                            task.processEntry(marshalledEntry, taskContext);
                        }
                    } catch (Exception e) {
                        throw new PersistenceException(e);
                    }
                }
            } finally {
                batchScanner.close();
            }
        } catch (TableNotFoundException e) {
            throw new PersistenceException("Unable to calculate size of Accumulo cache table " + tableName, e);
        }
    }
}
