package datawave.query.predicate;

import com.google.common.base.Predicate;
import com.google.common.collect.Maps;
import datawave.query.data.parsers.DatawaveKey;
import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.Document;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.EventDataQueryExpressionVisitor;
import datawave.typemetadata.TypeMetadata;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.Map;

/**
 * This class is used to filter out fields that are required for evaluation by apply the query expressions to the field values on the fly. This filter will
 * "keep" all of those returned by "apply". If more fields are required to be returned to the user, then this class must be overridden. startNewDocument will be
 * called with a documentKey whenever we are starting to scan a new document or document tree as defined by getKeyRange.
 */
public class EventDataQueryExpressionFilter implements EventDataQueryFilter {
    private static final Logger log = Logger.getLogger(EventDataQueryExpressionFilter.class);
    private Map<String,Predicate<Key>> filters = null;
    private boolean initialized = false;
    
    public EventDataQueryExpressionFilter() {
        super();
    }
    
    public EventDataQueryExpressionFilter(ASTJexlScript script, TypeMetadata metadata) {
        AttributeFactory attributeFactory = new AttributeFactory(metadata);
        Map<String,EventDataQueryExpressionVisitor.ExpressionFilter> expressionFilters = EventDataQueryExpressionVisitor.getExpressionFilters(script,
                        attributeFactory);
        setFilters(expressionFilters);
    }
    
    public EventDataQueryExpressionFilter(EventDataQueryExpressionFilter other) {
        setFilters(EventDataQueryExpressionVisitor.ExpressionFilter.clone(other.getFilters()));
        if (other.document != null) {
            document = new Key(other.document);
        }
    }
    
    protected Key document = null;
    
    @Override
    public void startNewDocument(Key document) {
        this.document = document;
        // since we are starting a new document, reset the filters
        EventDataQueryExpressionVisitor.ExpressionFilter.reset(filters);
    }
    
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
    
    protected void setFilters(Map<String,? extends Predicate<Key>> fieldFilters) {
        if (this.initialized) {
            throw new RuntimeException("This Projection instance was already initialized");
        }
        
        this.filters = Maps.newHashMap(fieldFilters);
        this.initialized = true;
    }
    
    protected Map<String,Predicate<Key>> getFilters() {
        return Collections.unmodifiableMap(this.filters);
    }
    
    @Override
    public boolean apply(Map.Entry<Key,String> input) {
        if (!this.initialized) {
            throw new RuntimeException("The EventDataQueryExpressionFilter was not initialized");
        }
        
        final DatawaveKey datawaveKey = new DatawaveKey(input.getKey());
        final String fieldName = JexlASTHelper.deconstructIdentifier(datawaveKey.getFieldName(), false);
        return this.filters.containsKey(fieldName) && this.filters.get(fieldName).apply(input.getKey());
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
        return new EventDataQueryExpressionFilter(this);
    }
}
