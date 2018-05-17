package datawave.query.rewrite.predicate;

import com.google.common.collect.Sets;
import datawave.query.rewrite.attributes.Document;
import datawave.query.rewrite.jexl.JexlASTHelper;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTJexlScript;

import java.util.Map;
import java.util.Set;

/**
 * This filter will filter event data keys by only those fields that are required in the specified query.
 */
public class EventDataQueryFieldFilter extends KeyProjection implements EventDataQueryFilter {
    
    public EventDataQueryFieldFilter() {
        super();
        // empty white list and black list
    }
    
    public EventDataQueryFieldFilter(EventDataQueryFieldFilter other) {
        super(other);
        if (other.document != null) {
            document = new Key(other.document);
        }
    }
    
    /**
     * Initialize the query field filter with all of the fields required to evaluation this query
     * 
     * @param script
     */
    public EventDataQueryFieldFilter(ASTJexlScript script) {
        Set<String> queryFields = Sets.newHashSet();
        for (ASTIdentifier identifier : JexlASTHelper.getIdentifiers(script)) {
            queryFields.add(JexlASTHelper.deconstructIdentifier(identifier));
        }
        
        initializeWhitelist(queryFields);
    }
    
    protected Key document = null;
    
    @Override
    public void setDocumentKey(Key document) {
        this.document = document;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see nsa.datawave.query.rewrite.predicate.Filter#keep(org.apache.accumulo.core.data.Key)
     */
    @Override
    public boolean keep(Key k) {
        return true;
    }
    
    @Override
    public Key getStartKey(Key from) {
        Key startKey = new Key(from.getRow(), from.getColumnFamily());
        return startKey;
    }
    
    @Override
    public Key getStopKey(Key from) {
        return from.followingKey(PartialKey.ROW_COLFAM);
    }
    
    @Override
    public Range getKeyRange(Map.Entry<Key,Document> from) {
        return new Range(getStartKey(from.getKey()), true, getStopKey(from.getKey()), false);
    }
    
    /**
     * Not yet implemented for this filter. Not guaranteed to be called
     *
     * @param current
     *            the current key at the top of the source iterator
     * @param endKey
     *            the current range endKey
     * @param endKeyInclusive
     *            the endKeyInclusive flag from the current range
     * @return
     */
    @Override
    public Range getSeekRange(Key current, Key endKey, boolean endKeyInclusive) {
        // not yet implemented
        return null;
    }
    
    @Override
    public int getMaxNextCount() {
        // not yet implemented
        return -1;
    }
    
    @Override
    public Key transform(Key toLimit) {
        // not yet implemented
        return null;
    }
    
    @Override
    public EventDataQueryFilter clone() {
        return new EventDataQueryFieldFilter(this);
    }
}
