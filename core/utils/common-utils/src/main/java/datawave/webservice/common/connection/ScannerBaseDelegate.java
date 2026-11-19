package datawave.webservice.common.connection;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.IteratorSetting.Column;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.clientImpl.ScannerOptions;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple wrapper around a {@link ScannerBase} that overrides the methods that configure iterators.
 * <p>
 * This class tracks "system" iterators locally to protect them from being cleared or removed by user code. System iterators are prefixed with
 * {@link #SYSTEM_ITERATOR_NAME_PREFIX}.
 */
public class ScannerBaseDelegate implements ScannerBase {
    private static final Logger log = LoggerFactory.getLogger(ScannerBaseDelegate.class);
    private static final String SYSTEM_ITERATOR_NAME_PREFIX = "sys_";

    protected final ScannerBase delegate;

    /** Tracks the names of system iterators added via {@link #addSystemScanIterator(IteratorSetting)} */
    private final Set<String> systemIteratorNames = new HashSet<>();

    /** Tracks the names of user iterators added via {@link #addScanIterator(IteratorSetting)} */
    private final Set<String> userIteratorNames = new HashSet<>();

    public ScannerBaseDelegate(ScannerBase delegate) {
        this.delegate = delegate;
    }

    @Override
    public ConsistencyLevel getConsistencyLevel() {
        return this.delegate.getConsistencyLevel();
    }

    @Override
    public void setConsistencyLevel(ConsistencyLevel level) {
        delegate.setConsistencyLevel(level);
    }

    @Override
    public void addScanIterator(IteratorSetting cfg) {
        if (cfg.getName().startsWith(SYSTEM_ITERATOR_NAME_PREFIX)) {
            throw new IllegalArgumentException("Non-system iterators' names cannot start with " + SYSTEM_ITERATOR_NAME_PREFIX);
        } else {
            delegate.addScanIterator(cfg);
            userIteratorNames.add(cfg.getName());
        }
    }

    /**
     * Adds a "system" scan iterator. The iterator name is automatically prefixed with {@link #SYSTEM_ITERATOR_NAME_PREFIX}. A "system" scan iterator can only
     * be modified or removed by calling {@link #updateSystemScanIteratorOption(String, String, String)}, {@link #removeSystemScanIterator(String)}, or
     * {@link #clearSystemScanIterators()}. Updates the iterator configuration for {@code iteratorName}. The iterator name is automatically prefixed with
     * {@link #SYSTEM_ITERATOR_NAME_PREFIX}.
     *
     * @param cfg
     *            the configuration of the iterator to add
     */
    public void addSystemScanIterator(IteratorSetting cfg) {
        if (!cfg.getName().startsWith(SYSTEM_ITERATOR_NAME_PREFIX)) {
            cfg.setName(SYSTEM_ITERATOR_NAME_PREFIX + cfg.getName());
        }
        delegate.addScanIterator(cfg);
        systemIteratorNames.add(cfg.getName());
    }

    @Override
    public void removeScanIterator(String iteratorName) {
        if (iteratorName.startsWith(SYSTEM_ITERATOR_NAME_PREFIX)) {
            throw new IllegalArgumentException("DATAWAVE system iterator " + iteratorName + " cannot be removed");
        } else {
            delegate.removeScanIterator(iteratorName);
            userIteratorNames.remove(iteratorName);
        }
    }

    /**
     * Removes a "system" scan iterator. The iterator name is automatically prefixed with {@link #SYSTEM_ITERATOR_NAME_PREFIX}.
     *
     * @param iteratorName
     *            the name of the system iterator to remove
     */
    public void removeSystemScanIterator(String iteratorName) {
        if (!iteratorName.startsWith(SYSTEM_ITERATOR_NAME_PREFIX)) {
            iteratorName = SYSTEM_ITERATOR_NAME_PREFIX + iteratorName;
        }
        delegate.removeScanIterator(iteratorName);
        systemIteratorNames.remove(iteratorName);
    }

    @Override
    public void updateScanIteratorOption(String iteratorName, String key, String value) {
        if (iteratorName.startsWith(SYSTEM_ITERATOR_NAME_PREFIX)) {
            throw new IllegalArgumentException("DATAWAVE system iterator " + iteratorName + " cannot be updated");
        } else {
            delegate.updateScanIteratorOption(iteratorName, key, value);
        }
    }

    /**
     * Updates the iterator configuration for {@code iteratorName}. The iterator name is automatically prefixed with {@link #SYSTEM_ITERATOR_NAME_PREFIX}.
     *
     * @param iteratorName
     *            the name of the system iterator to modify
     * @param key
     *            the name of the iterator option to modify
     * @param value
     *            the new value for the iterator option named in {@code key}
     */
    public void updateSystemScanIteratorOption(String iteratorName, String key, String value) {
        if (!iteratorName.startsWith(SYSTEM_ITERATOR_NAME_PREFIX)) {
            iteratorName = SYSTEM_ITERATOR_NAME_PREFIX + iteratorName;
        }
        delegate.updateScanIteratorOption(iteratorName, key, value);
    }

    @Override
    public void fetchColumnFamily(Text col) {
        delegate.fetchColumnFamily(col);
    }

    @Override
    public void fetchColumn(Text colFam, Text colQual) {
        delegate.fetchColumn(colFam, colQual);
    }

    @Override
    public void fetchColumn(Column column) {
        delegate.fetchColumn(column);
    }

    @Override
    public void clearColumns() {
        delegate.clearColumns();
    }

    @Override
    public void clearScanIterators() {
        // NOTE: ScannerOptions is a non-public Accumulo API. This instanceof check is intentionally retained as a
        // safety guard until all usages of ScannerOptions as a delegate are fully replaced (e.g. with SessionOptions).
        if (!(delegate instanceof ScannerOptions)) {
            throw new UnsupportedOperationException("Cannot clear scan iterators on a non-ScannerOptions class! (" + delegate.getClass() + ")");
        }
        // Remove all user iterators (tracked locally), preserving system iterators
        for (String iteratorName : userIteratorNames) {
            delegate.removeScanIterator(iteratorName);
        }
        userIteratorNames.clear();
    }

    /**
     * Clears all iterators (including system iterators).
     */
    public void clearSystemScanIterators() {
        delegate.clearScanIterators();
        systemIteratorNames.clear();
        userIteratorNames.clear();
    }

    @Override
    public Iterator<Map.Entry<Key,Value>> iterator() {
        return delegate.iterator();
    }

    @Override
    public void setTimeout(long timeOut, TimeUnit timeUnit) {
        delegate.setTimeout(timeOut, timeUnit);
    }

    @Override
    public long getTimeout(TimeUnit timeUnit) {
        return delegate.getTimeout(timeUnit);
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public Authorizations getAuthorizations() {
        return delegate.getAuthorizations();
    }

    @Override
    public void setSamplerConfiguration(SamplerConfiguration samplerConfiguration) {
        delegate.setSamplerConfiguration(samplerConfiguration);
    }

    @Override
    public SamplerConfiguration getSamplerConfiguration() {
        return delegate.getSamplerConfiguration();
    }

    @Override
    public void clearSamplerConfiguration() {
        delegate.clearSamplerConfiguration();
    }

    @Override
    public void setBatchTimeout(long l, TimeUnit timeUnit) {
        delegate.setBatchTimeout(l, timeUnit);
    }

    @Override
    public long getBatchTimeout(TimeUnit timeUnit) {
        return delegate.getBatchTimeout(timeUnit);
    }

    @Override
    public void setClassLoaderContext(String s) {
        delegate.setClassLoaderContext(s);
    }

    @Override
    public void clearClassLoaderContext() {
        delegate.clearClassLoaderContext();
    }

    @Override
    public String getClassLoaderContext() {
        return delegate.getClassLoaderContext();
    }

    public void setContext(String context) {
        delegate.setClassLoaderContext(context);
    }

    public void clearContext() {
        delegate.clearClassLoaderContext();
    }

    public String getContext() {
        return delegate.getClassLoaderContext();
    }

    @Override
    public void setExecutionHints(Map<String,String> hints) {
        delegate.setExecutionHints(hints);
    }

}
