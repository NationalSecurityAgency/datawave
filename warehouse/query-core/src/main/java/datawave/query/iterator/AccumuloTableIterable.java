package datawave.query.iterator;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import datawave.query.function.DocumentRangeProvider;
import datawave.query.function.Equality;
import datawave.query.function.PrefixEquality;
import datawave.query.function.RangeProvider;
import datawave.query.predicate.EventDataQueryFilter;
import datawave.query.attributes.Document;
import datawave.query.iterator.aggregation.DocumentData;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.log4j.Logger;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;

/**
 * An iterable interface for wrapping Documents directly from the "shard" table. Also referred to as the "full-table query"
 *
 */
public class AccumuloTableIterable extends AccumuloTreeIterable<Key,DocumentData> {
    private static final Logger log = Logger.getLogger(AccumuloTableIterable.class);
    
    private final SortedKeyValueIterator<Key,Value> source;
    private final Predicate<Key> filter;
    private final Equality eq;
    private final IteratorEnvironment environment;
    private final EventDataQueryFilter evaluationFilter;
    private boolean includeChildCount = false;
    private boolean includeParent = false;
    private final Map<String,String> options;
    
    private Range totalRange;
    private RangeProvider rangeProvider;
    
    public AccumuloTableIterable(SortedKeyValueIterator<Key,Value> source, Predicate<Key> filter, boolean includeChildCount, boolean includeParent) {
        this(source, null, null, filter, new PrefixEquality(PartialKey.ROW_COLFAM), null, includeChildCount, includeParent);
    }
    
    public AccumuloTableIterable(SortedKeyValueIterator<Key,Value> source, Predicate<Key> filter, Equality eq, boolean includeChildCount,
                    boolean includeParent) {
        this(source, null, null, filter, eq, null, includeChildCount, includeParent);
    }
    
    public AccumuloTableIterable(SortedKeyValueIterator<Key,Value> source, Predicate<Key> filter, Equality eq, EventDataQueryFilter evaluationFilter,
                    boolean includeChildCount, boolean includeParent) {
        this(source, null, null, filter, eq, evaluationFilter, includeChildCount, includeParent);
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    public AccumuloTableIterable(final SortedKeyValueIterator<Key,Value> source, final IteratorEnvironment env, final Map<String,String> options,
                    Predicate<Key> filter, Equality eq, EventDataQueryFilter evaluationFilter, boolean includeChildCount, boolean includeParent) {
        this.source = source;
        this.environment = env;
        this.options = (null != options) ? options : (Map) Collections.emptyMap();
        this.filter = filter;
        this.eq = eq;
        this.evaluationFilter = evaluationFilter;
        this.includeChildCount = includeChildCount;
        this.includeParent = includeParent;
    }
    
    @Override
    public Iterator<Entry<DocumentData,Document>> iterator() {
        final DocumentDataIterator docItr = new DocumentDataIterator(this.source, this.environment, this.options, this.totalRange, this.filter, this.eq,
                        this.evaluationFilter, getRangeProvider(), this.includeChildCount, this.includeParent);
        
        return Iterators.transform(docItr, from -> Maps.immutableEntry(from, new Document()));
    }
    
    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        if (!range.isInfiniteStartKey() && !range.isStartKeyInclusive()) {
            Key startKey = range.getStartKey();
            Key newStartKey = startKey.followingKey(PartialKey.ROW_COLFAM);
            
            if (log.isTraceEnabled()) {
                log.trace("Permuting start key to attempt to avoid duplicative returned key. Was:" + startKey + ", Now:" + newStartKey);
            }
            
            range = new Range(newStartKey, false, range.getEndKey(), range.isEndKeyInclusive());
        }
        
        this.totalRange = range;
        this.source.seek(range, columnFamilies, inclusive);
    }
    
    protected RangeProvider getRangeProvider() {
        if (rangeProvider == null) {
            rangeProvider = new DocumentRangeProvider();
        }
        return rangeProvider;
    }
}
