package datawave.query.predicate;

import datawave.query.attributes.Document;
import datawave.typemetadata.TypeMetadata;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.jexl2.parser.ASTJexlScript;

import java.util.Map;

public class ConfigurableEventDataQueryFilter implements EventDataQueryFilter {
    
    private final EventDataQueryFilter filter;
    
    protected Key document = null;
    
    public ConfigurableEventDataQueryFilter(ASTJexlScript script, TypeMetadata metadata, boolean expressionFilterEnabled) {
        if (expressionFilterEnabled) {
            filter = new EventDataQueryExpressionFilter(script, metadata);
        } else {
            filter = new EventDataQueryFieldFilter(script);
        }
    }
    
    public ConfigurableEventDataQueryFilter(ConfigurableEventDataQueryFilter other) {
        filter = other.filter.clone();
        if (other.document != null) {
            document = new Key(other.document);
        }
    }
    
    @Override
    public void startNewDocument(Key document) {
        this.document = document;
        filter.startNewDocument(document);
    }
    
    @Override
    public Key getStartKey(Key from) {
        // don't delegate to the filter, we need an implementation here to override subclasses.
        return new Key(from.getRow(), from.getColumnFamily());
    }
    
    @Override
    public Key getStopKey(Key from) {
        // don't delegate to the filter, we need an implementation here to override in subclasses.
        return from.followingKey(PartialKey.ROW_COLFAM);
    }
    
    @Override
    public Range getKeyRange(Map.Entry<Key,Document> from) {
        // don't delegate to the filter, we need an implementation here to override in subclasses.
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
        return filter.getSeekRange(current, endKey, endKeyInclusive);
    }
    
    @Override
    public int getMaxNextCount() {
        return filter.getMaxNextCount();
    }
    
    @Override
    public boolean keep(Key k) {
        return filter.keep(k);
    }
    
    @Override
    public boolean apply(Map.Entry<Key,String> input) {
        return filter.apply(input);
    }
    
    @Override
    public Key transform(Key toLimit) {
        return filter.transform(toLimit);
    }
    
    @Override
    public EventDataQueryFilter clone() {
        return new ConfigurableEventDataQueryFilter(this);
    }
}
