package datawave.webservice.common.connection;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.IteratorSetting.Column;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.clientImpl.ScannerOptions;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.thrift.IterInfo;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * A simple wrapper around a {@link ScannerBase} that overrides the methods that configure iterators.
 * 
 * @see ScannerOptionsHelper
 */
public class ScannerBaseDelegate implements ScannerBase {
    private static final Logger log = LoggerFactory.getLogger(ScannerBaseDelegate.class);
    private static final String SYSTEM_ITERATOR_NAME_PREFIX = "sys_";
    
    protected final ScannerBase delegate;
    
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
    }
    
    @Override
    public void removeScanIterator(String iteratorName) {
        if (iteratorName.startsWith(SYSTEM_ITERATOR_NAME_PREFIX)) {
            throw new IllegalArgumentException("DATAWAVE system iterator " + iteratorName + " cannot be removed");
        } else {
            delegate.removeScanIterator(iteratorName);
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
        if (delegate instanceof ScannerOptions) {
            ScannerOptionsHelper opts = new ScannerOptionsHelper((ScannerOptions) delegate);
            for (IteratorSetting iteratorSetting : opts.getIterators()) {
                if (!iteratorSetting.getName().startsWith(SYSTEM_ITERATOR_NAME_PREFIX)) {
                    delegate.removeScanIterator(iteratorSetting.getName());
                }
            }
        } else {
            throw new UnsupportedOperationException("Cannot clear scan iterators on a non-ScannerOptions class! (" + delegate.getClass() + ")");
        }
    }
    
    /**
     * Clears all iterators (including system iterators).
     */
    public void clearSystemScanIterators() {
        delegate.clearScanIterators();
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
    
    private static class ScannerOptionsHelper extends ScannerOptions {
        
        public ScannerOptionsHelper(ScannerOptions other) {
            super(other);
        }
        
        public Collection<IteratorSetting> getIterators() {
            Collection<IteratorSetting> settings = Lists.newArrayList();
            for (IterInfo iter : serverSideIteratorList) {
                IteratorSetting setting = new IteratorSetting(iter.getPriority(), iter.getIterName(), iter.getClassName());
                setting.addOptions(serverSideIteratorOptions.get(iter.getIterName()));
                settings.add(setting);
            }
            return settings;
        }
        
    }
    
    @Override
    public void setExecutionHints(Map<String,String> hints) {
        delegate.setExecutionHints(hints);
    }
    
}
