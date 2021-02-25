package datawave.iterators;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import datawave.ingest.table.aggregator.PropogatingCombiner;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.conf.ColumnToClassMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * Purpose: Handle arbitrary propogating aggregations.
 * 
 * Design: Though very similar to the DeletingIterator, due to private methods and members, we cannot directly extend the DeletingIterator. As a result, the
 * class extends SKVI. This class {@code USES --> PropogatingAggregator}. Note that propAgg can be null
 * 
 * Initially the TotalAggregatingIterator, this class was a direct copy. At some point it was identified that there was an artifact where deletes would not be
 * propogated. As a result, this class becomes nearly identical to the DeletingIterator, whereby deletes are always propogated until a full major compaction.
 */
public class PropogatingIterator implements SortedKeyValueIterator<Key,Value>, OptionDescriber {
    
    public static final String ATTRIBUTE_NAME = "agg";
    
    public static final String ATTRIBUTE_DESCRIPTION = "Aggregators apply aggregating functions to values with identical keys. You can specify the column family. DEFAULT matches the default locality group";
    
    public static final String UNNAMED_OPTION_DESCRIPTION = "<Column Family> <Combiner> <optional: combOpt1=comVal1;combOpt2=combVal2...>";
    
    public static final String AGGREGATOR_DEFAULT = "DEFAULT";
    
    public static final String AGGREGATOR_DEFAULT_OPT = "*";
    
    public static final Map<String,String> defaultMapOptions;
    
    static {
        defaultMapOptions = Maps.newHashMap();
        defaultMapOptions.put(AGGREGATOR_DEFAULT, "default aggregator class");
    }
    
    /**
     * Underlying iterator.
     */
    protected SortedKeyValueIterator<Key,Value> iterator;
    
    /**
     * current working key
     */
    protected Key workKey = new Key();
    
    /**
     * Aggregated key and value.
     */
    protected Key aggrKey;
    protected Value aggrValue;
    
    /**
     * Current iterator environment.
     */
    protected IteratorEnvironment env;
    
    /**
     * Propogating iterator
     */
    protected PropogatingCombiner defaultAgg = null;
    protected Map<ByteSequence,PropogatingCombiner> aggMap;
    
    /**
     * Combiner options so that we can effectively deep copy
     */
    protected Map<String,String> options = Maps.newHashMap();
    
    private static final Logger log = LoggerFactory.getLogger(PropogatingIterator.class);
    
    /**
     * Deep copy implementation
     */
    public PropogatingIterator deepCopy(IteratorEnvironment env) {
        return new PropogatingIterator(this, env);
    }
    
    /**
     * Private constructor.
     */
    private PropogatingIterator(PropogatingIterator other, IteratorEnvironment env) {
        iterator = other.iterator.deepCopy(env);
        this.env = env;
        if (null != defaultAgg)
            defaultAgg = (PropogatingCombiner) other.defaultAgg.deepCopy(env);
        this.aggMap = Maps.newHashMap();
        options.putAll(other.options);
        // this will configure us and deep copy safely
        validateOptions(options);
    }
    
    public PropogatingIterator() {
        aggMap = Maps.newHashMap();
    }
    
    /**
     * Aggregates the same partial key.
     */
    private boolean aggregateRowColumn() {
        // this function assumes that first value is not delete
        
        workKey.set(iterator.getTopKey());
        
        final Key keyToAggregate = workKey;
        
        PropogatingCombiner aggr = getAggregator(workKey);
        
        Value aggregatedValue = new Value(iterator.getTopValue());
        
        // always propogate deletes
        if (aggr != null) {
            
            log.trace("aggregator is not null");
            
            // reset the state of the combiner.
            aggr.reset();
            
            aggregatedValue = aggr.reduce(keyToAggregate, new ValueCombiner(iterator));
            
            if (aggr.propogateKey() || workKey.isDeleted()) {
                log.trace("propogating {}", workKey);
                aggrKey = workKey;
            } else {
                log.trace("Not propogating {}", workKey);
                return false;
            }
        }
        
        aggrKey = new Key(workKey);
        
        aggrValue = aggregatedValue;
        
        return true;
        
    }
    
    private PropogatingCombiner getAggregator(Key key) {
        PropogatingCombiner aggr = aggMap.get(key.getColumnFamilyData());
        
        if (null == aggr) {
            log.trace("using the default aggregator");
            aggr = defaultAgg;
        }
        
        log.trace("Key is {}", key);
        
        log.trace("{}agg == {} {}", key, (aggr == null), key.isDeleted());
        return aggr;
    }
    
    /**
     * Find Top method, will attempt to aggregate, iff an aggregator is specified
     */
    private void findTop() {
        // check if aggregation is needed
        while (iterator.hasTop() && !aggregateRowColumn())
            ;
        
    }
    
    /**
     * SKVI Constructor
     */
    public PropogatingIterator(SortedKeyValueIterator<Key,Value> iterator, ColumnToClassMapping<Combiner> Aggregators) {
        this.iterator = iterator;
        findTop();
    }
    
    @Override
    public Key getTopKey() {
        if (aggrKey != null) {
            return aggrKey;
        }
        return iterator.getTopKey();
    }
    
    @Override
    public Value getTopValue() {
        if (aggrKey != null) {
            return aggrValue;
        }
        return iterator.getTopValue();
    }
    
    @Override
    public boolean hasTop() {
        return aggrKey != null || iterator.hasTop();
    }
    
    @Override
    public void next() throws IOException {
        if (aggrKey != null) {
            // if aggrKey isn't configured for aggregation, then we previously didn't call next and need to now
            if (iterator.hasTop() && getAggregator(aggrKey) == null) {
                iterator.next();
            }
            aggrKey = null;
            aggrValue = null;
        }
        findTop();
    }
    
    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        if (aggrKey != null) {
            aggrKey = null;
            aggrValue = null;
        }
        
        Range seekRange = range;
        // if there isn't an aggregator configured for the start key, timestamp modification isn't necessary
        if (range.getStartKey() != null && getAggregator(range.getStartKey()) != null) {
            // do not want to seek to the middle of a value that should be
            // aggregated...
            seekRange = IteratorUtil.maximizeStartKeyTimeStamp(range);
        }
        
        iterator.seek(seekRange, columnFamilies, inclusive);
        
        findTop();
        
        // (only if the range was modified) it's necessary to skip keys until the start key is found
        if (seekRange != range) {
            while (hasTop() && getTopKey().equals(range.getStartKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS)
                            && getTopKey().getTimestamp() > range.getStartKey().getTimestamp()) {
                next();
            }
            
            while (hasTop() && range.beforeStartKey(getTopKey())) {
                
                next();
            }
        }
        
    }
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        if (null != source)
            this.iterator = source.deepCopy(env);
        else
            this.iterator = null;
        this.env = env;
        this.options.putAll(options);
        validateOptions(options);
        
    }
    
    @Override
    public IteratorOptions describeOptions() {
        
        return new IteratorOptions(ATTRIBUTE_NAME, ATTRIBUTE_DESCRIPTION, defaultMapOptions, Collections.singletonList(UNNAMED_OPTION_DESCRIPTION));
    }
    
    @Override
    public boolean validateOptions(Map<String,String> options) {
        // fail if environment is null
        Preconditions.checkNotNull(env);
        Preconditions.checkNotNull(options);
        
        boolean shouldPropogate = !(env.getIteratorScope() == IteratorScope.majc && env.isFullMajorCompaction());
        
        options.forEach((name, value) -> {
            value = value.trim();
            int sepIdx = value.indexOf(' ');
            String aggClass = (sepIdx < 0) ? value : value.substring(0, sepIdx);
            Object agg = createAggregator(aggClass);
            if (agg instanceof PropogatingCombiner) {
                PropogatingCombiner propAgg = (PropogatingCombiner) agg;
                if (sepIdx > 0) {
                    String encodedOpts = value.substring(sepIdx + 1);
                    Map<String,String> aggOpts = Splitter.on(';').trimResults().withKeyValueSeparator('=').split(encodedOpts);
                    propAgg.validateOptions(aggOpts);
                }
                propAgg.setPropogate(shouldPropogate);
                if (name.equals(AGGREGATOR_DEFAULT) || name.equals(AGGREGATOR_DEFAULT_OPT)) {
                    log.trace("Default aggregator is {}", propAgg.getClass());
                    defaultAgg = propAgg;
                } else {
                    aggMap.put(new ArrayByteSequence(name.getBytes()), propAgg);
                }
            }
        });
        return true;
    }
    
    /**
     * Create the aggregator using the provided options.
     *
     * @param className
     * @return
     */
    private Object createAggregator(String className) {
        try {
            return this.getClass().getClassLoader().loadClass(className).newInstance();
        } catch (Exception e) {
            throw new IllegalArgumentException("Exception while attempting to create : " + className, e);
        }
    }
}
