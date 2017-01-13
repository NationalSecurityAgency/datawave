package nsa.datawave.query.rewrite.predicate;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.Vector;
import java.util.Map.Entry;

import nsa.datawave.query.rewrite.attributes.Document;
import nsa.datawave.query.rewrite.function.Equality;
import nsa.datawave.query.rewrite.function.KeyToDocumentData;
import nsa.datawave.query.rewrite.function.PrefixEquality;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.lang3.StringUtils;
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
     * Given a Key pointing to the start of an document to aggregate, construct a Range that should encapsulate the "document" to be aggregated together. Also
     * checks to see if data was found for the constructed Range before returning.
     * 
     * @param documentKey
     *            A Key of the form "bucket type\x00uid: "
     * @deprecated replaced by the non-static instance method
     * @return
     */
    @Deprecated
    public List<Entry<Key,Value>> collectAttributesForDocumentKey(Key documentStartKey, SortedKeyValueIterator<Key,Value> source, Equality equality,
                    EventDataQueryFilter filter, Set<Key> docKeys) throws IOException {
        
        final List<Entry<Key,Value>> documentAttributes;
        if (null == documentStartKey) {
            documentAttributes = Collections.emptyList();
        } else {
            documentAttributes = new Vector<>(50);
            WeakReference<Key> docAttrKey = new WeakReference<>(source.getTopKey());
            
            while (equality.partOf(documentStartKey, docAttrKey.get())) {
                
                if (filter == null || filter.apply(Maps.immutableEntry(docAttrKey.get(), StringUtils.EMPTY))) {
                    docKeys.add(getDocKey(docAttrKey.get()));
                    documentAttributes.add(Maps.immutableEntry(docAttrKey.get(), source.getTopValue()));
                }
                
                source.next();
                
                if (source.hasTop()) {
                    docAttrKey = new WeakReference<>(source.getTopKey());
                } else {
                    break;
                }
                
            }
        }
        
        return documentAttributes;
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
