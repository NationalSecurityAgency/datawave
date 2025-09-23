package datawave.query.tables;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import datawave.query.config.ShardQueryConfiguration;
import datawave.util.TextUtil;

/**
 * An implementation of {@link ScannerBase} to be used instead of {@link org.apache.accumulo.core.clientImpl.ScannerOptions} since that class is not part of
 * Accumulo's public API.
 */
public class SessionOptions implements ScannerBase {

    protected List<IterInfo> scanIterators = Collections.emptyList();
    protected Map<String,Map<String,String>> scanIteratorOptions = Collections.emptyMap();
    protected SortedSet<Column> fetchedColumns = new TreeSet<>();
    protected long retryTimeout = Long.MAX_VALUE;
    protected long batchTimeout = Long.MAX_VALUE;
    protected SamplerConfiguration samplerConfig = null;
    protected String classLoaderContext = null;
    protected Map<String,String> executionHints = Collections.emptyMap();
    protected ConsistencyLevel consistencyLevel = ConsistencyLevel.IMMEDIATE;
    protected ShardQueryConfiguration queryConfig;

    protected ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    public SessionOptions() {}

    public SessionOptions(SessionOptions options) {
        setOptions(this, options);
    }

    protected static void setOptions(SessionOptions dst, SessionOptions src) {
        synchronized (dst) {
            synchronized (src) {
                dst.scanIterators = new ArrayList<>(src.scanIterators);
                dst.scanIteratorOptions = new HashMap<>();
                if (!src.scanIteratorOptions.isEmpty()) {
                    src.scanIteratorOptions.entrySet().forEach(e -> dst.scanIteratorOptions.put(e.getKey(), new HashMap<>(e.getValue())));
                }
                dst.fetchedColumns = new TreeSet<>(src.fetchedColumns);
                dst.retryTimeout = src.retryTimeout;
                dst.batchTimeout = src.batchTimeout;
                dst.samplerConfig = src.samplerConfig;
                dst.classLoaderContext = src.classLoaderContext;
                // executionHints is an immutable map, no copying required.
                dst.executionHints = src.executionHints;
                dst.consistencyLevel = src.consistencyLevel;
                dst.queryConfig = src.queryConfig;
            }
        }
    }

    @Override
    public synchronized void addScanIterator(IteratorSetting iterator) {
        requireNonNull(iterator, "iterator setting is null");
        if (scanIterators.isEmpty()) {
            scanIterators = new ArrayList<>();
        }

        // Verify an iterator has not already been added with the same name or priority.
        for (IterInfo info : scanIterators) {
            if (info.name.equals(iterator.getName())) {
                throw new IllegalArgumentException("Iterator name is already in use: " + iterator.getName());
            }
            if (info.priority == iterator.getPriority()) {
                throw new IllegalArgumentException("Iterator priority is already in use: " + iterator.getPriority());
            }
        }

        scanIterators.add(new IterInfo(iterator));

        if (scanIteratorOptions.isEmpty()) {
            scanIteratorOptions = new HashMap<>();
        }
        scanIteratorOptions.computeIfAbsent(iterator.getName(), name -> new HashMap<>()).putAll(iterator.getOptions());
    }

    @Override
    public synchronized void removeScanIterator(String iteratorName) {
        requireNonNull(iteratorName, "iterator name is null");
        if (!scanIterators.isEmpty()) {
            for (IterInfo info : scanIterators) {
                if (info.name.equals(iteratorName)) {
                    scanIterators.remove(info);
                    break;
                }
            }
            scanIteratorOptions.remove(iteratorName);
        }
    }

    @Override
    public synchronized void clearScanIterators() {
        scanIterators = Collections.emptyList();
        scanIteratorOptions = Collections.emptyMap();
    }

    /**
     * Returns a copy of the server-side iterators in this {@link SessionOptions}.
     *
     * @return the iterators
     */
    public List<IteratorSetting> getIterators() {
        // @formatter:off
        return scanIterators.stream()
                        .map(info -> new IteratorSetting(info.priority, info.name, info.iteratorClass, scanIteratorOptions.get(info.name)))
                        .collect(Collectors.toList());
        // @formatter:on
    }

    @Override
    public void updateScanIteratorOption(String iteratorName, String key, String value) {
        requireNonNull(iteratorName, "iterator name is null");
        requireNonNull(key, "key is null");
        requireNonNull(value, "value is null");
        scanIteratorOptions.computeIfAbsent(iteratorName, name -> new HashMap<>()).put(key, value);
    }

    @Override
    public synchronized void fetchColumnFamily(Text colFam) {
        requireNonNull(colFam, "column family is null");
        Column column = new Column(TextUtil.getBytes(colFam), null, null);
        fetchedColumns.add(column);
    }

    @Override
    public synchronized void fetchColumn(Text colFam, Text colQual) {
        requireNonNull(colFam, "column family is null");
        requireNonNull(colQual, "column qualifier is null");
        Column column = new Column(TextUtil.getBytes(colFam), TextUtil.getBytes(colQual), null);
        fetchedColumns.add(column);
    }

    @Override
    public void fetchColumn(IteratorSetting.Column column) {
        requireNonNull(column, "column is null");
        fetchColumn(column.getColumnFamily(), column.getColumnQualifier());
    }

    /**
     * Returns the set of columns that will be fetched.
     *
     * @return
     */
    public synchronized SortedSet<Column> getFetchedColumns() {
        return fetchedColumns;
    }

    @Override
    public synchronized void clearColumns() {
        this.fetchedColumns.clear();
    }

    @Override
    public Iterator<Map.Entry<Key,Value>> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTimeout(long timeout, TimeUnit timeUnit) {
        if (timeout < 0) {
            throw new IllegalArgumentException("retry timeout must be a positive: " + timeout);
        }
        if (timeout == 0) {
            this.retryTimeout = Long.MAX_VALUE;
        } else {
            this.retryTimeout = timeUnit.toMillis(timeout);
        }
    }

    @Override
    public long getTimeout(TimeUnit timeUnit) {
        return timeUnit.convert(retryTimeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        // Nothing needs to be closed.
    }

    @Override
    public Authorizations getAuthorizations() {
        throw new UnsupportedOperationException("No authorizations to return");
    }

    @Override
    public synchronized void setSamplerConfiguration(SamplerConfiguration samplerConfig) {
        this.samplerConfig = requireNonNull(samplerConfig, "sampler config cannot be null");
    }

    @Override
    public synchronized SamplerConfiguration getSamplerConfiguration() {
        return samplerConfig;
    }

    @Override
    public synchronized void clearSamplerConfiguration() {
        this.samplerConfig = null;
    }

    @Override
    public void setBatchTimeout(long timeout, TimeUnit timeUnit) {
        if (timeout < 0) {
            throw new IllegalArgumentException("batch timeout must be a positive: " + timeout);
        }
        if (timeout == 0) {
            this.batchTimeout = Long.MAX_VALUE;
        } else {
            this.batchTimeout = timeUnit.toMillis(timeout);
        }
    }

    @Override
    public long getBatchTimeout(TimeUnit timeUnit) {
        return timeUnit.convert(batchTimeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public void setClassLoaderContext(String classLoaderContext) {
        this.classLoaderContext = requireNonNull(classLoaderContext, "class loader context cannot be null");
    }

    @Override
    public void clearClassLoaderContext() {
        this.classLoaderContext = null;
    }

    @Override
    public String getClassLoaderContext() {
        return this.classLoaderContext;
    }

    @Override
    public void setExecutionHints(Map<String,String> hints) {
        this.executionHints = Map.copyOf(requireNonNull(hints, "hints cannot be null"));
    }

    public void applyExecutionHints(Map<String,String> scanHints) {
        setExecutionHints(scanHints);
    }

    public void applyExecutionHints(String tableName, Map<String,Map<String,String>> tableScanHints) {
        if (tableScanHints.containsKey(tableName)) {
            setExecutionHints(tableScanHints.get(tableName));
        }
    }

    public void applyConsistencyLevel(ConsistencyLevel consistencyLevel) {
        setConsistencyLevel(consistencyLevel);
    }

    public void applyConsistencyLevel(String tableName, Map<String,ConsistencyLevel> consistencyLevels) {
        if (consistencyLevels.containsKey(tableName)) {
            setConsistencyLevel(consistencyLevels.get(tableName));
        }
    }

    @Override
    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    @Override
    public void setConsistencyLevel(ConsistencyLevel level) {
        this.consistencyLevel = requireNonNull(level, "consistency level cannot be null");
    }

    /**
     * Returns the query configuration that will be used by this scanner.
     *
     * @return the configuration
     */
    public ShardQueryConfiguration getQueryConfiguration() {
        return queryConfig;
    }

    /**
     * Set the query configuration that will be used by this scanner.
     *
     * @param queryConfig
     *            the configuration
     */
    public void setQueryConfiguration(ShardQueryConfiguration queryConfig) {
        this.queryConfig = queryConfig;
    }

    protected class IterInfo {
        private final String name;
        private final String iteratorClass;
        private final int priority;

        public IterInfo(IteratorSetting iterator) {
            this.name = iterator.getName();
            this.iteratorClass = iterator.getIteratorClass();
            this.priority = iterator.getPriority();
        }

        public String getName() {
            return name;
        }

        public String getIteratorClass() {
            return iteratorClass;
        }

        public int getPriority() {
            return priority;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            IterInfo iterInfo = (IterInfo) o;
            return priority == iterInfo.priority && Objects.equals(name, iterInfo.name) && Objects.equals(iteratorClass, iterInfo.iteratorClass);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, iteratorClass, priority);
        }
    }

}
