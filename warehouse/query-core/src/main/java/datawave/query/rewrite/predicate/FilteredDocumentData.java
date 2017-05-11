package datawave.query.rewrite.predicate;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.Vector;
import java.util.Map.Entry;

import datawave.query.rewrite.attributes.Document;
import datawave.query.rewrite.function.Equality;
import datawave.query.rewrite.function.KeyToDocumentData;
import datawave.query.rewrite.function.PrefixEquality;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class FilteredDocumentData extends KeyToDocumentData {
    
    private static final Logger log = Logger.getLogger(FilteredDocumentData.class);
    
    protected boolean limitMap = false;
    
    String first = null, last = null;
    
    public FilteredDocumentData(SortedKeyValueIterator<Key,Value> source) {
        
        this(source, new PrefixEquality(PartialKey.ROW_COLFAM), false, false);
    }
    
    public FilteredDocumentData(SortedKeyValueIterator<Key,Value> source, Equality equality, boolean includeChildCount, boolean includeParent) {
        super(source, equality, includeChildCount, includeParent);
    }
    
    public FilteredDocumentData(SortedKeyValueIterator<Key,Value> source, Equality equality, EventDataQueryFilter sourceFilter, boolean includeChildCount,
                    boolean includeParent) {
        this(source, null, null, equality, sourceFilter, includeChildCount, includeParent);
    }
    
    public FilteredDocumentData(SortedKeyValueIterator<Key,Value> source, IteratorEnvironment env, Map<String,String> options, Equality equality,
                    EventDataQueryFilter sourceFilter, boolean includeChildCount, boolean includeParent) {
        super(source, env, options, equality, sourceFilter, includeChildCount, includeParent);
        SortedSet<String> fieldProjections = Sets.newTreeSet();
        fieldProjections.addAll(sourceFilter.getProjection().getWhitelist());
        first = Iterables.getFirst(fieldProjections, null);
        last = Iterables.getLast(fieldProjections, null) + "\uffff";
    }
    
    /**
     * Define the start key given the from condition.
     * 
     * @param from
     * @return
     */
    @Override
    protected Key getStartKey(Entry<Key,Document> from) {
        if (null == first)
            return super.getStartKey(from);
        else
            return new Key(from.getKey().getRow().toString(), from.getKey().getColumnFamily().toString(), first);
    }
    
    /**
     * Define the end key given the from condition.
     * 
     * @param from
     * @return
     */
    @Override
    protected Key getStopKey(Entry<Key,Document> from) {
        if (null == last) {
            return super.getStopKey(from);
        } else
            
            return new Key(from.getKey().getRow().toString(), from.getKey().getColumnFamily().toString(), last);
    }
    
    /**
     * Get the key range.
     * 
     * @param from
     * @return
     */
    @Override
    protected Range getKeyRange(Entry<Key,Document> from) {
        return new Range(getStartKey(from), true, getStopKey(from), false);
    }
}
