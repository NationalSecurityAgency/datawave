package nsa.datawave.core.iterators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import nsa.datawave.core.iterators.uid.ShardUidMappingIterator;
import nsa.datawave.query.config.GenericShardQueryConfiguration;
import nsa.datawave.query.enrich.DataEnricher;
import nsa.datawave.query.enrich.EnrichingMaster;
import nsa.datawave.query.rewrite.Constants;
import nsa.datawave.util.StringUtils;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.log4j.Logger;

/**
 * This iterator internally uses the BooleanLogicIterator to find event UIDs in the field index portion of the shard and uses the
 * MinimizingEventEvaluatingIterator to evaluate the events against an expression. The key and value that are emitted from this iterator are the key and value
 * that come from the EvaluatingIterator.
 */
@Deprecated
public class ShardEventOptimizationIterator implements SortedKeyValueIterator<Key,Value>, OptionDescriber {
    
    private static Logger log = Logger.getLogger(ShardEventOptimizationIterator.class);
    public static final String CONTAINS_UNEVALUATED_FIELDS = "CONTAINS_UNEVALUATED_FIELDS";
    private EvaluatingIterator event = null;
    private SortedKeyValueIterator<Key,Value> index = null;
    private EnrichingMaster enricher = null;
    private Key key = null;
    private Value value = null;
    private boolean eventSpecificRange = false;
    private boolean forceBooleanLogic = false;
    private boolean uidMapping = false;
    private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<>();
    
    @Override
    public IteratorOptions describeOptions() {
        Map<String,String> options = new HashMap<>();
        
        options.put(EnrichingMaster.UNEVALUATED_FIELDS, "List of index-only fields");
        options.put(EnrichingMaster.ENRICHMENT_CLASSES, "List of enrichment classes to apply to query results");
        
        options.putAll(new ShardUidMappingIterator().describeOptions().getNamedOptions());
        options.putAll(new EvaluatingIterator().describeOptions().getNamedOptions());
        options.putAll(new BooleanLogicIteratorJexl().describeOptions().getNamedOptions());
        options.putAll(new ReadAheadIterator().describeOptions().getNamedOptions());
        
        return new IteratorOptions(getClass().getSimpleName(), "evaluates event objects against an expression using the field index", options, null);
    }
    
    @Override
    public boolean validateOptions(Map<String,String> options) {
        if (options.containsKey(EvaluatingIterator.QUERY_OPTION) && options.containsKey(BooleanLogicIteratorJexl.FIELD_INDEX_QUERY)) {
            // Uid mapping is optional
            uidMapping = new ShardUidMappingIterator().validateOptions(options);
            
            if (!new EvaluatingIterator().validateOptions(options)) {
                return false;
            }
            
            if (!new BooleanLogicIteratorJexl().validateOptions(options)) {
                return false;
            }
            
            if (options.containsKey(ReadAheadIterator.QUEUE_SIZE) && options.containsKey(ReadAheadIterator.TIMEOUT)) {
                if (!new ReadAheadIterator().validateOptions(options)) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        long t = System.currentTimeMillis();
        if (!validateOptions(options)) {
            throw new IOException("Invalid options");
        }
        
        event = new EvaluatingIterator();
        event.init(source.deepCopy(env), options, env);
        
        // if uid mapping, then setup a uid mapping iterator. Note that the event (EvaluatingIterator) takes
        // care of uid mapping directory so the underlying source is used there.
        SortedKeyValueIterator<Key,Value> mappingIterator = null;
        if (uidMapping) {
            mappingIterator = new ShardUidMappingIterator();
            mappingIterator.init(source.deepCopy(env), options, env);
        } else {
            mappingIterator = source.deepCopy(env);
        }
        
        // if queue size and timeout are set, then use the read ahead iterator
        if (options.containsKey(ReadAheadIterator.QUEUE_SIZE) && options.containsKey(ReadAheadIterator.TIMEOUT)) {
            BooleanLogicIteratorJexl bli = new BooleanLogicIteratorJexl();
            bli.init(mappingIterator, options, env);
            index = new ReadAheadIterator();
            index.init(bli, options, env);
        } else {
            index = new BooleanLogicIteratorJexl();
            index.init(mappingIterator, options, env);
        }
        
        this.forceBooleanLogic = options.containsKey(CONTAINS_UNEVALUATED_FIELDS);
        
        // If we want to enable any enricher
        if (options.containsKey(EnrichingMaster.ENRICHMENT_ENABLED)) {
            String query = options.get(EnrichingMaster.QUERY);
            String fields = options.get(EnrichingMaster.UNEVALUATED_FIELDS);
            String classes = options.get(EnrichingMaster.ENRICHMENT_CLASSES);
            
            String[] classNames;
            if (null == classes) {
                classNames = new String[0];
            } else {
                classNames = StringUtils.split(classes, GenericShardQueryConfiguration.PARAM_VALUE_SEP);
            }
            
            String[] unevaluatedFields;
            if (null == fields) {
                unevaluatedFields = new String[0];
            } else {
                unevaluatedFields = StringUtils.split(fields, GenericShardQueryConfiguration.PARAM_VALUE_SEP);
            }
            
            Map<String,Object> enricherOptions = new HashMap<>();
            enricherOptions.put("query", query);
            enricherOptions.put("unevaluatedFields", unevaluatedFields);
            
            // Warn if no classes were provided (doesn't *need* to fail)
            if (classNames.length == 0) {
                log.debug("No enrichment will occur because no class names were provided. At least one class name should be provided");
            }
            
            // Warn if no unevaluatedFields were provided (doesn't *need* to fail)
            if (unevaluatedFields.length == 0) {
                log.debug("No unevaluated fields were provided to the enricher");
            }
            
            // TODO: should we use the source here or the mappingIterator
            enricher = new EnrichingMaster(source.deepCopy(env), env, classNames, enricherOptions);
        }
        if (log.isDebugEnabled()) {
            log.debug("init_time took: " + (System.currentTimeMillis() - t) + " millis");
        }
    }
    
    public ShardEventOptimizationIterator() {}
    
    public ShardEventOptimizationIterator(ShardEventOptimizationIterator other, IteratorEnvironment env) {
        this.event = other.event;
        this.index = other.index;
        this.enricher = other.enricher;
        this.uidMapping = other.uidMapping;
        this.forceBooleanLogic = other.forceBooleanLogic;
    }
    
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        return new ShardEventOptimizationIterator(this, env);
    }
    
    @Override
    public Key getTopKey() {
        if (log.isDebugEnabled()) {
            log.debug("getTopKey: " + key);
        }
        return key;
    }
    
    @Override
    public Value getTopValue() {
        return value;
    }
    
    @Override
    public boolean hasTop() {
        if (log.isDebugEnabled()) {
            log.debug("hasTop: returned: " + (key != null));
        }
        return (key != null);
    }
    
    @Override
    public void next() throws IOException {
        log.debug("next()");
        if (key != null) {
            key = null;
            value = null;
        }
        
        if (eventSpecificRange && !this.forceBooleanLogic) {
            log.debug("calling event.next()");
            // Then this will probably return nothing
            long event_execution_time = System.currentTimeMillis();
            event.next();
            event_execution_time = System.currentTimeMillis() - event_execution_time;
            if (log.isDebugEnabled()) {
                log.debug("event_execution_time.next() took: " + event_execution_time + " millis");
            }
            if (event.hasTop()) {
                key = event.getTopKey();
                value = event.getTopValue();
            }
        } else {
            
            do {
                log.debug("calling bool_index.next()");
                long bool_execution_time = System.currentTimeMillis();
                index.next();
                bool_execution_time = System.currentTimeMillis() - bool_execution_time;
                if (log.isDebugEnabled()) {
                    log.debug("boolean_logic.next() took: " + bool_execution_time + "  millis");
                }
                // If the index has a match, then seek the event to the key
                if (index.hasTop()) {
                    Range eventRange = new Range(index.getTopKey(), index.getTopKey().followingKey(PartialKey.ROW_COLFAM));
                    long event_execution_time = System.currentTimeMillis();
                    event.seek(eventRange, EMPTY_COL_FAMS, false);
                    event_execution_time = System.currentTimeMillis() - event_execution_time;
                    if (log.isDebugEnabled()) {
                        log.debug("event_execution_time.seek() took: " + event_execution_time + " millis");
                    }
                    if (event.hasTop()) {
                        key = event.getTopKey();
                        value = event.getTopValue();
                    }
                }
            } while (key == null && index.hasTop());
        }
        // Sanity check. Make sure both returnValue and returnKey are null or both are not null
        if (!((key == null && value == null) || (key != null && value != null))) {
            log.warn("Key: " + ((key == null) ? "null" : key.toString()));
            log.warn("Value: " + ((value == null) ? "null" : value.toString()));
            throw new IOException("Return values are inconsistent");
        }
        
        if (enricher != null && key != null) {
            enrichTopKeyAndValue();
        }
    }
    
    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("seek, range:" + range);
            log.debug("forceBooleanLogic: " + forceBooleanLogic);
        }
        
        // Test the range to see if it is event specific.
        // The endKey should always match the event-specific range versus shard/date range.
        // When a shard/date range scan is interrupted, it will be re-seeked to a specific event
        // (the last event the scan returned before being interrupted), but the endKey in the range
        // will still be the same that was initially set.
        if (null != range.getEndKey() && range.getEndKey().getColumnFamily() != null && range.getEndKey().getColumnFamily().getLength() != 0
                        && range.getEndKey().getColumnFamily().find(Constants.MAX_UNICODE_STRING) == -1 && !this.forceBooleanLogic) {
            if (log.isDebugEnabled()) {
                log.debug("Jumping straight to the event");
            }
            // Then this range is for a specific event. We don't need to use the index iterator to find it, we can just
            // seek to it with the event iterator and evaluate it.
            eventSpecificRange = true;
            long event_execution_time = System.currentTimeMillis();
            event.seek(range, columnFamilies, inclusive);
            event_execution_time = System.currentTimeMillis() - event_execution_time;
            if (log.isDebugEnabled()) {
                log.debug("event_execution_time.seek() took: " + event_execution_time + " millis");
            }
            if (event.hasTop()) {
                key = event.getTopKey();
                value = event.getTopValue();
                if (log.isDebugEnabled()) {
                    log.debug("seek, event key: " + key);
                }
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("seek, Could not find event or it didn't match the query.");
                }
                key = null;
                value = null;
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Using BooleanLogicIteratorJexl");
            }
            eventSpecificRange = false;
            long bool_execution_time = System.currentTimeMillis();
            index.seek(range, columnFamilies, inclusive);
            bool_execution_time = System.currentTimeMillis() - bool_execution_time;
            if (log.isDebugEnabled()) {
                log.debug("boolean_logic.seek() took: " + bool_execution_time + "  millis");
            }
            // If the index has a match, then seek the event to the key
            if (index.hasTop()) {
                Key eventKey = index.getTopKey();
                Value eventValue = index.getTopValue();
                
                long event_execution_time = System.currentTimeMillis();
                event.seekEvent(eventKey, eventValue);
                event_execution_time = System.currentTimeMillis() - event_execution_time;
                if (log.isDebugEnabled()) {
                    log.debug("event_execution_time.seek() took: " + event_execution_time + " millis");
                }
                if (event.hasTop()) {
                    key = event.getTopKey();
                    value = event.getTopValue();
                } else {
                    next();
                }
            } else {
                this.key = null;
                this.value = null;
            }
        }
        
        if (enricher != null && key != null) {
            enrichTopKeyAndValue();
        }
    }
    
    /**
     * Given that {@link key} is non-null, this will run all of the configured {@link DataEnricher}s over the topKey/topValue of this iterator
     *
     * It updates the topKey/topValue with the enriched key/value. If the enricher returns a null key for the current enrich method, the topKey is discarded,
     * and next() is called on the iterator. Meaning, this method will always try to make a topKey if possible
     *
     * @throws IOException
     */
    private void enrichTopKeyAndValue() throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("Enriching the key: " + this.key);
        }
        
        // Ensure that the key is non-null (we could be ignoring the value)
        if (this.key == null) {
            if (log.isDebugEnabled()) {
                log.debug("Not enrich the current key as it is null");
            }
            
            return;
        }
        
        // Enrich this key-value
        enricher.enrich(this.key, this.value);
        
        // Enrichers cannot modify the key as it may alter the sorted order
        value = enricher.getValue();
    }
}
