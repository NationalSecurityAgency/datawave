package nsa.datawave.query.rewrite.predicate;

import java.util.Map;
import java.util.Set;

import nsa.datawave.query.rewrite.attributes.Document;
import nsa.datawave.query.rewrite.jexl.JexlASTHelper;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTJexlScript;

import com.google.common.collect.Sets;

/**
 * This filter will filter event data keys by only those fields that are required in the specified query.
 */
public class EventDataQueryFilter extends KeyProjection implements Filter, SeekingFilter {
    
    public EventDataQueryFilter() {
        super();
        // empty white list and black list
    }
    
    /**
     * Initialize the query field filter with all of the fields required to evaluation this query
     * 
     * @param script
     */
    public EventDataQueryFilter(ASTJexlScript script) {
        Set<String> queryFields = Sets.newHashSet();
        for (ASTIdentifier identifier : JexlASTHelper.getIdentifiers(script)) {
            queryFields.add(JexlASTHelper.deconstructIdentifier(identifier));
        }
        
        initializeWhitelist(queryFields);
    }
    
    protected Key document = null;
    
    /**
     * This method can be used to change the document context fo the keep(Key k) method.
     * 
     * @param document
     */
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
    
    /**
     * Define the start key given the from condition.
     *
     * @param from
     * @return
     */
    public Key getStartKey(Key from) {
        Key startKey = new Key(from.getRow(), from.getColumnFamily());
        return startKey;
    }
    
    /**
     * Define the end key given the from condition.
     *
     * @param from
     * @return
     */
    public Key getStopKey(Key from) {
        return from.followingKey(PartialKey.ROW_COLFAM);
    }
    
    /**
     * Get the key range that covers the complete document specified by the input key range
     *
     * @param from
     * @return
     */
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
}
