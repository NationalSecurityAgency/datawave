package nsa.datawave.mr.bulk;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.log4j.Logger;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import nsa.datawave.mr.bulk.split.TabletSplitSplit;
import nsa.datawave.query.tables.SessionOptions;
import nsa.datawave.security.iterator.ConfigurableVisibilityFilter;
import nsa.datawave.security.util.AuthorizationsUtil;

public class RfileScanner extends SessionOptions implements BatchScanner, Closeable {
    
    private static final Logger log = Logger.getLogger(RfileScanner.class);
    
    private List<Range> ranges;
    private String table;
    private Configuration conf;
    
    protected List<RecordIterator> iterators;
    
    protected Set<Authorizations> auths;
    
    protected Iterator<Authorizations> authIter;
    
    protected AtomicBoolean resought = new AtomicBoolean(false);
    
    protected AtomicBoolean closed = new AtomicBoolean(false);
    
    private static Cache<String,AccumuloConfiguration> tableConfigMap = CacheBuilder.newBuilder().maximumSize(100).concurrencyLevel(100)
                    .expireAfterAccess(24, TimeUnit.HOURS).build();
    
    private static Cache<String,String> tableIdMap = CacheBuilder.newBuilder().maximumSize(100).concurrencyLevel(100).expireAfterAccess(24, TimeUnit.HOURS)
                    .build();
    
    protected Connector connector;
    
    public RfileScanner(Connector connector, Configuration conf, String table, Set<Authorizations> auths, int numQueryThreads) {
        ArgumentChecker.notNull(connector, conf, table, auths);
        this.table = table;
        this.conf = new Configuration(conf);
        this.auths = auths;
        this.connector = connector;
        ranges = null;
        authIter = AuthorizationsUtil.minimize(auths).iterator();
        conf.setBoolean(MultiRfileInputformat.CACHE_METADATA, true);
        conf.set("recorditer.auth.string", authIter.next().toString());
        conf.getInt(RecordIterator.RECORDITER_FAILURE_COUNT_MAX, RecordIterator.FAILURE_MAX_DEFAULT);
        iterators = Lists.newArrayList();
        iterators = Collections.synchronizedList(iterators);
        
    }
    
    public void setConfiguration(Configuration conf) {
        this.conf = conf;
    }
    
    @Override
    public void setRanges(Collection<Range> ranges) {
        if (ranges == null || ranges.size() == 0) {
            throw new IllegalArgumentException("ranges must be non null and contain at least 1 range");
        }
        
        this.ranges = Lists.newArrayList(ranges);
        
    }
    
    protected void addVisibilityFilters(Iterator<Authorizations> iter) {
        for (int priority = 10; iter.hasNext(); priority++) {
            IteratorSetting cfg = new IteratorSetting(priority, ConfigurableVisibilityFilter.class);
            cfg.setName("visibilityFilter" + priority);
            cfg.addOption(ConfigurableVisibilityFilter.AUTHORIZATIONS_OPT, iter.next().toString());
            BulkInputFormat.addIterator(conf, cfg);
        }
    }
    
    public void seek(Range range) throws IOException {
        seek(range, Collections.<ByteSequence> emptyList(), false);
    }
    
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        for (RecordIterator ri : iterators) {
            ri.seek(range, columnFamilies, inclusive);
        }
        resought.set(true);
    }
    
    protected Iterator<Entry<Key,Value>> getIterator(List<InputSplit> splits, AccumuloConfiguration acuTableConf) {
        // optimization for single tablets
        Iterator<Entry<Key,Value>> kv = Iterators.emptyIterator();
        for (InputSplit split : splits) {
            RecordIterator recordIter = null;
            
            recordIter = new RecordIterator((TabletSplitSplit) split, acuTableConf, conf);
            
            iterators.add(recordIter);
            
            kv = Iterators.concat(kv, new RfileIterator(recordIter));
            
        }
        
        return kv;
    }
    
    @Override
    public Iterator<Entry<Key,Value>> iterator() {
        
        Iterator<Entry<Key,Value>> kv = Iterators.emptyIterator();
        try {
            if (resought.get()) {
                resought.set(false);
                for (RecordIterator recordIterator : iterators) {
                    kv = Iterators.concat(kv, new RfileIterator(recordIterator));
                }
                return kv;
            }
            if (ranges == null) {
                throw new IllegalStateException("ranges not set");
            }
            addVisibilityFilters(authIter);
            List<InputSplit> splits;
            for (IteratorSetting setting : getIterators()) {
                BulkInputFormat.addIterator(conf, setting);
            }
            
            final long failureSleep = conf.getLong(RecordIterator.RECORDITER_FAILURE_SLEEP_INTERVAL, RecordIterator.DEFAULT_FAILURE_SLEEP);
            try {
                splits = MultiRfileInputformat.computeSplitPoints(connector, conf, table, ranges);
                String tableId = tableIdMap.getIfPresent(table);
                if (null == tableId) {
                    tableId = Tables.getTableId(connector.getInstance(), table);
                    tableIdMap.put(table, tableId);
                }
                AccumuloConfiguration acuTableConf = tableConfigMap.getIfPresent(tableId);
                if (null == acuTableConf) {
                    acuTableConf = AccumuloConfiguration.getTableConfiguration(connector, tableId);
                    tableConfigMap.put(tableId, acuTableConf);
                }
                
                int maxRetries = conf.getInt(RecordIterator.RECORDITER_FAILURE_COUNT_MAX, RecordIterator.FAILURE_MAX_DEFAULT);
                int retries = 0;
                do {
                    if (closed.get())
                        break;
                    try {
                        kv = getIterator(splits, acuTableConf);
                    } catch (Exception e) {
                        
                        close();
                        
                        // an exception has occurred that won't allow us to open the files. perhaps one was moved
                        // immediately upon opening the tablet.
                        if (++retries > maxRetries) {
                            log.info("Giving up because" + retries + " >= " + maxRetries);
                            throw e;
                        }
                        
                        MultiRfileInputformat.clearMetadataCache();
                        
                        Thread.sleep(failureSleep);
                        
                        splits = MultiRfileInputformat.computeSplitPoints(connector, conf, table, ranges);
                        
                    }
                } while (null == kv);
                
            } catch (Exception e) {
                IOUtils.cleanup(null, this);
                throw new RuntimeException(e);
            }
        } finally {
            /**
             * This is required because Hadoop will swallow the interrupt. As a result we must notify ourselves that the interrupt occurred. In doing so we call
             * a subsequent close here since we weren't interrupted in the call to getIterator(...).
             */
            if (closed.get()) {
                close();
            }
        }
        return kv;
        
    }
    
    @Override
    public void close() {
        /**
         * This is required because Hadoop will swallow the interrupt. As a result we must notify ourselves that the interrupt occurred. In doing so we call a
         * subsequent close here since we weren't interrupted in the call to getIterator(...).
         */
        closed.set(true);
        
        for (RecordIterator iter : iterators) {
            try {
                iter.close();
            } catch (IOException e) {
                log.error(e);
            }
        }
        iterators = Lists.newArrayList();
    }
}
