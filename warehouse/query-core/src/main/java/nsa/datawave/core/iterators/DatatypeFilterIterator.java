package nsa.datawave.core.iterators;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import nsa.datawave.util.StringUtils;
import nsa.datawave.query.config.GenericShardQueryConfiguration;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SkippingIterator;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 * <p>
 * Iterator that can efficiently return a filtered set of event keys, specified by datatype, from a DATAWAVE "sharded" event table.
 * </p>
 * 
 * <p>
 * Takes a list of {@link GenericShardQueryConfiguration#PARAM_VALUE_SEP} separated list of datatypes whose event keys should be returned via the option
 * {@code DatatypeFilterIterator.DATATYPE_FILTER}.
 * </p>
 * 
 * <p>
 * Will automatically skip over in-partition index keys (fi, tf, d)
 * </p>
 * 
 * 
 * 
 */
@Deprecated
public class DatatypeFilterIterator extends SkippingIterator implements OptionDescriber {
    public static final String DATATYPE_FILTER = "datatypes";
    private TreeSet<String> acceptDatatypes = new TreeSet<>();
    private static final String NULL = "\0";
    private final Logger log = Logger.getLogger(DatatypeFilterIterator.class);
    
    private int id = this.hashCode();
    private Range prevRange = new Range();
    @SuppressWarnings("unchecked")
    private Collection<ByteSequence> columnFamilies = (Set<ByteSequence>) Collections.EMPTY_SET;
    private boolean colfInclusive = false;
    
    private boolean pastEnd = false;
    
    private final Text prevRow = new Text();
    private final Text prevCf = new Text();
    private final Text cf = new Text();
    
    public DatatypeFilterIterator() {
        
    }
    
    public DatatypeFilterIterator(DatatypeFilterIterator other, IteratorEnvironment env) {
        setSource(other.getSource().deepCopy(env));
        
        this.acceptDatatypes = other.acceptDatatypes;
    }
    
    public DatatypeFilterIterator deepCopy(IteratorEnvironment env) {
        return new DatatypeFilterIterator(this, env);
    }
    
    @Override
    protected void consume() throws IOException {
        if (log.isTraceEnabled()) {
            log.trace(id + " ----> consume()");
        }
        
        // We only want to loop as long as there is more data
        while (getSource().hasTop() && prevRange.contains(getSource().getTopKey())) {
            final Key topKey = getTopKey();
            this.cf.set(topKey.getColumnFamily().getBytes());
            
            // If it's the same cf, we don't need to re-check
            if (topKey.getRow().compareTo(prevRow) == 0 && this.cf.compareTo(prevCf) == 0) {
                return;
            }
            
            final int index = this.cf.find(NULL);
            
            if (log.isTraceEnabled()) {
                log.trace(id + " ----> index = " + index);
            }
            
            // No null byte in cf ('tf' or 'd' column family)
            if (index == -1) {
                if (log.isTraceEnabled()) {
                    log.trace(id + " ----> no null byte");
                }
                
                final String nextDatatype = acceptDatatypes.higher(cf.toString());
                
                final Key k;
                if (null == nextDatatype) {
                    if (log.isTraceEnabled()) {
                        log.trace(id + " ----> done seeking this row");
                    }
                    
                    k = new Key(topKey.followingKey(PartialKey.ROW).getRow(), new Text(acceptDatatypes.first() + "\0"));
                } else {
                    k = new Key(topKey.getRow(), new Text(nextDatatype + "\0"));
                }
                
                if (prevRange.afterEndKey(k)) {
                    pastEnd = true;
                    return;
                }
                
                Range r = new Range(k, prevRange.isStartKeyInclusive(), prevRange.getEndKey(), prevRange.isEndKeyInclusive());
                
                if (log.isTraceEnabled()) {
                    log.trace(id + " ----> seeking to: " + r);
                    log.trace(id + " ----> Next type: " + nextDatatype);
                }
                
                getSource().seek(r, this.columnFamilies, this.colfInclusive);
            } else {
                // Is an event or field index key
                final String prefix = Text.decode(this.cf.getBytes(), 0, index);
                if (log.isTraceEnabled()) {
                    log.trace(id + " ----> prefix: " + prefix);
                }
                
                // If the prefix (datatype) is in our set, return it
                if (acceptDatatypes.contains(prefix)) {
                    if (log.isTraceEnabled()) {
                        log.trace(id + " ----> accepting key");
                    }
                    
                    prevRow.set(topKey.getRow().getBytes(), 0, topKey.getRow().getLength());
                    prevCf.set(cf.getBytes(), 0, cf.getLength());
                    
                    return;
                } else {
                    if (log.isTraceEnabled()) {
                        log.trace(id + " ----> has null byte, not in accept set");
                    }
                    
                    final String nextDatatype = acceptDatatypes.higher(prefix);
                    
                    final Key k;
                    if (null == nextDatatype) {
                        if (log.isTraceEnabled()) {
                            log.trace(id + " ----> done seeking this row");
                        }
                        k = new Key(topKey.followingKey(PartialKey.ROW).getRow(), new Text(acceptDatatypes.first() + "\0"));
                    } else {
                        k = new Key(topKey.getRow(), new Text(nextDatatype + "\0"));
                    }
                    
                    if (prevRange.afterEndKey(k)) {
                        if (log.isTraceEnabled()) {
                            log.trace(id + " ----> done seeking");
                        }
                        pastEnd = true;
                        return;
                    }
                    
                    final Range r = new Range(k, prevRange.isStartKeyInclusive(), prevRange.getEndKey(), prevRange.isEndKeyInclusive());
                    if (log.isTraceEnabled()) {
                        log.trace(id + " ----> Want to seek to: " + k);
                        log.trace(id + " ----> Next type: " + nextDatatype);
                        log.trace(id + " ----> seeking to: " + r);
                    }
                    
                    getSource().seek(r, this.columnFamilies, this.colfInclusive);
                    
                    if (log.isTraceEnabled()) {
                        log.trace(id + " ----> seeked to: " + r);
                    }
                }
            }
        }
    }
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        validateOptions(options);
        
        super.init(source, options, env);
    }
    
    @Override
    public boolean hasTop() {
        if (pastEnd) {
            if (log.isTraceEnabled()) {
                log.trace(id + " ----> hasTop called when past range");
            }
            return false;
        }
        
        return super.hasTop();
    }
    
    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        this.prevRange = range;
        this.columnFamilies = columnFamilies;
        this.colfInclusive = inclusive;
        
        super.seek(range, columnFamilies, inclusive);
    }
    
    @Override
    public IteratorOptions describeOptions() {
        Map<String,String> options = new HashMap<>();
        
        options.put(DatatypeFilterIterator.DATATYPE_FILTER, "'" + GenericShardQueryConfiguration.PARAM_VALUE_SEP + "' separated list of datatypes to filter on");
        
        return new IteratorOptions(getClass().getSimpleName(), "Efficiently filters out all keys for datatypes not in the provided list", options, null);
        
    }
    
    @Override
    public boolean validateOptions(Map<String,String> options) {
        if (!options.containsKey(DATATYPE_FILTER) || null == options.get(DATATYPE_FILTER)) {
            log.error("Options did not contain: " + DATATYPE_FILTER);
            return false;
        }
        
        String datatypeString = options.get(DATATYPE_FILTER);
        
        List<String> datatypeList = Arrays.asList(StringUtils.split(datatypeString, GenericShardQueryConfiguration.PARAM_VALUE_SEP));
        
        acceptDatatypes = new TreeSet<>(datatypeList);
        
        if (log.isTraceEnabled()) {
            log.trace("Configured datatypes: " + acceptDatatypes);
        }
        
        return true;
    }
}
