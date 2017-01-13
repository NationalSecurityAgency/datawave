package nsa.datawave.core.iterators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;

import nsa.datawave.query.iterators.JumpingIterator;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * An iterator that handles "OR" query constructs on the server side. This code has been adapted/merged from Heap and Multi Iterators.
 */
@Deprecated
public class OrIteratorJexl implements JumpingIterator<Key,Value> {
    
    private DatawaveFieldIndexIteratorJexl currentTerm;
    private ArrayList<DatawaveFieldIndexIteratorJexl> termSources;
    private Key topKey = null;
    protected static final Logger log = Logger.getLogger(OrIteratorJexl.class);
    private SortedKeyValueIterator<Key,Value> source;
    private Multimap<Text,Text> fieldNamesAndValues;
    private Comparator<DatawaveFieldIndexIteratorJexl> comparator = new Comparator<DatawaveFieldIndexIteratorJexl>() {
        /**
         * This comparator is primarily in support of the OrIteratorJexl to allow dedupping results. We want to dedup based
         */
        @Override
        public int compare(DatawaveFieldIndexIteratorJexl o1, DatawaveFieldIndexIteratorJexl o2) {
            
            // if we both have a top key, compare them up through the column family
            if (o1.hasTop() && o2.hasTop()) {
                Key k = o2.getTopKey();
                return o1.getTopKey().compareTo(k, PartialKey.ROW_COLFAM);
            }
            
            // If my topKey is null, and the iter I'm being compared to has a top key, I'm greater
            if (!o1.hasTop() && o2.hasTop()) {
                return 1;
            }
            
            if (o1.hasTop() && !o2.hasTop()) {
                return -1;
            }
            
            // Otherwise we are both null
            return 0;
        }
    };
    private PriorityQueue<DatawaveFieldIndexIteratorJexl> sorted = new PriorityQueue<>(5, comparator);
    
    public OrIteratorJexl(Multimap<Text,Text> fieldNamesAndValues) {
        this.fieldNamesAndValues = fieldNamesAndValues;
        this.termSources = new ArrayList<>();
    }
    
    private OrIteratorJexl(OrIteratorJexl other, IteratorEnvironment env) throws IOException {
        this.termSources = new ArrayList<>();
        this.fieldNamesAndValues = HashMultimap.create();
        this.fieldNamesAndValues.putAll(other.getFieldNamesAndValues());
        
        // The OrIterator never does anything directly with this source, but deep copy just in case
        // we should expect the underlying iterators to deep copy on their own.
        _init(source, null, env);
    }
    
    private void _init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        this.source = source.deepCopy(env);
        DatawaveFieldIndexIteratorJexl iter;
        for (Entry<Text,Text> entry : this.fieldNamesAndValues.entries()) {
            iter = new DatawaveFieldIndexIteratorJexl(entry.getKey(), entry.getValue(), null, null);
            iter.init(this.source.deepCopy(env), null, env);
            this.termSources.add(iter);
        }
    }
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        _init(source, options, env);
    }
    
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        try {
            return new OrIteratorJexl(this, env);
        } catch (IOException e) {
            log.error("Could not instaniate OrIteratorJexl!", e);
        }
        return null;
    }
    
    public Multimap<Text,Text> getFieldNamesAndValues() {
        return this.fieldNamesAndValues;
    }
    
    /**
     * Calls to next should find the next, minimum event key.
     *
     * currentTerm is not in the sorted priority queue and it is the current top key and hence the minimum. We need to call next on it and push it back into the
     * priority queue to re-sort and poll to get the new minimum.
     *
     * It is possible that another FieldIndexIterator was also set to the same event key (using a different fieldname and value) so we must loop over this
     * process until the topkey actually changes.
     *
     * @throws IOException
     */
    @Override
    public void next() throws IOException {
        if (log.isTraceEnabled()) {
            log.trace("OI.next Enter: sorted.size = " + sorted.size() + " currentTerm = " + ((currentTerm == null) ? "null" : "not null"));
        }
        
        if (currentTerm == null || !currentTerm.hasTop()) {
            log.trace("OI.next currentTerm is NULL or does not have a top key, returning");
            topKey = null;
            return;
        }
        
        // We start by calling next on the currentTerm which is not currently
        // in the priority queue, then push it back in to re-sort and poll again
        // for the minimum. We repeat this until the topkey changes because it
        // is possible for other underlying FieldIndexIterators to have found
        // the same topKey via other FieldName->FieldValue.
        while (true) {
            if (log.isTraceEnabled()) {
                log.trace("next, currentTerm.next(): " + currentTerm);
            }
            currentTerm.next();
            if (currentTerm.hasTop()) {
                sorted.add(currentTerm);
            }
            
            if (sorted.isEmpty()) {
                topKey = null;
                return;
            }
            
            currentTerm = sorted.poll();
            if (log.isTraceEnabled()) {
                log.debug("next, min currentTerm=sorted.poll(): " + currentTerm);
            }
            // if our keys differ, then return otherwise continue, we don't
            // want to return duplicates. NOTE: We are comparing EVENT keys.
            if (currentTerm.getTopKey().compareTo(topKey, PartialKey.ROW_COLFAM) != 0) {
                topKey = currentTerm.getTopKey();
                return;
            }
        }
    }
    
    /**
     * Given a seek range, clear out our priority queue, call seek on all underlying FieldIndexIterators which will search the various FieldName->FieldValue
     * parts of our query, and push them back into the priority queue if they have a top key. Poll the priority queue to get the minimum topkey.
     *
     * The underlying iterators will handle all of the FieldIndex logic, pass them the unmodified seek range.
     *
     * The underlying iterators will return Event keys, as will this iterator.
     *
     * @param range
     * @param columnFamilies
     * @param inclusive
     * @throws IOException
     */
    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        if (log.isTraceEnabled()) {
            log.trace("OI.seek Entry - sources.size = " + termSources.size());
            log.trace("OI.seek Entry - currentTerm = " + ((currentTerm == null) ? "false" : currentTerm.getTopKey()));
            log.trace("OI.seek Entry - Key from Range = " + ((range == null) ? "false" : range.getStartKey()));
        }
        
        // If termSources.size is 0, there is nothing to process, so just return.
        if (termSources.isEmpty()) {
            currentTerm = null;
            topKey = null;
            return;
        }
        
        // Clear the PriorityQueue so that we can re-populate it.
        sorted.clear();
        
        for (DatawaveFieldIndexIteratorJexl iter : termSources) {
            if (log.isTraceEnabled()) {
                log.trace("TermSource: " + iter);
            }
            iter.seek(range, columnFamilies, inclusive);
            if (iter.hasTop()) {
                sorted.add(iter);
            }
        }
        
        if (sorted.isEmpty()) {
            currentTerm = null;
            topKey = null;
            return;
        }
        
        currentTerm = sorted.poll(); // do NOT put this back in the priority queue.
        if (log.isTraceEnabled()) {
            log.trace("seek, currentTerm: " + currentTerm);
        }
        this.topKey = currentTerm.getTopKey();
        
    }
    
    final public Text getCurrentFieldValue() {
        if (currentTerm == null) {
            if (log.isDebugEnabled()) {
                log.debug("getCurrentFieldValue(): currentTerm (iterator) is null, returning null");
            }
            return null;
        }
        return this.currentTerm.getfValue();
    }
    
    final public Text getCurrentFieldName() {
        if (currentTerm == null) {
            if (log.isDebugEnabled()) {
                log.debug("getCurrentFieldName(): currentTerm (iterator) is null, returning null");
            }
            return null;
        }
        return this.currentTerm.getFiName();
    }
    
    @Override
    final public Key getTopKey() {
        if (log.isTraceEnabled()) {
            log.trace("OI.getTopKey key >>" + topKey);
        }
        
        return (topKey == null ? topKey : new Key(topKey.getRow(), topKey.getColumnFamily()));
    }
    
    @Override
    final public Value getTopValue() {
        if (log.isTraceEnabled()) {
            log.trace("OI.getTopValue key >>" + currentTerm.getTopValue());
        }
        
        return currentTerm.getTopValue();
    }
    
    @Override
    final public boolean hasTop() {
        if (log.isTraceEnabled()) {
            log.trace("OI.hasTop  =  " + ((topKey == null) ? "false" : "true"));
        }
        
        return topKey != null;
    }
    
    /**
     * jump is basically seek except that it will not rewind an iterator if it is already past the jump key.
     *
     * jumpKey should be an EVENT key. The underlying iterators will handle turning it into a FieldIndex key. Since all iterators may be moving, we need to
     * rebuild the priority queue.
     *
     * The underlying iterators will perform all jump related logic/tests. We could test here, but we would end up performing the underlying logic twice.
     *
     * @param jumpKey
     * @return
     * @throws IOException
     */
    @Override
    public boolean jump(Key jumpKey) throws IOException {
        if (log.isTraceEnabled()) {
            log.trace("jump: " + jumpKey);
        }
        
        if (this.hasTop() && this.topKey.compareTo(jumpKey) < 0) {
            sorted.clear();
            
            for (DatawaveFieldIndexIteratorJexl iter : this.termSources) {
                if (log.isTraceEnabled()) {
                    log.trace("jumping, " + iter);
                }
                iter.jump(jumpKey);
                if (iter.hasTop()) {
                    sorted.add(iter);
                }
            }
            
            currentTerm = null;
            if (sorted.isEmpty()) {
                this.topKey = null;
            } else {
                currentTerm = sorted.poll();
                if (log.isTraceEnabled()) {
                    log.trace("jump, min currentTerm: " + currentTerm);
                }
                this.topKey = currentTerm.getTopKey();
            }
        }
        
        return hasTop();
    }
    
}
