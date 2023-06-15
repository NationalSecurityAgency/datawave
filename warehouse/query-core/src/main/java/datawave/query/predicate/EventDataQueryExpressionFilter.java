package datawave.query.predicate;

import com.google.common.collect.Maps;
import datawave.query.data.parsers.DatawaveKey;
import datawave.query.attributes.AttributeFactory;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.EventDataQueryExpressionVisitor;
import datawave.query.util.TypeMetadata;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * This class is used to filter out fields that are required for evaluation by apply the query expressions to the field values on the fly. This filter will
 * "keep" all of those returned by "apply". If more fields are required to be returned to the user, then this class must be overridden. startNewDocument will be
 * called with a documentKey whenever we are starting to scan a new document or document tree.
 */
public class EventDataQueryExpressionFilter implements EventDataQueryFilter {
    private static final Logger log = Logger.getLogger(EventDataQueryExpressionFilter.class);
    private Map<String,PeekingPredicate<Key>> filters = null;
    private boolean initialized = false;
    private Set<String> nonEventFields;
    
    public EventDataQueryExpressionFilter() {
        super();
    }
    
    public EventDataQueryExpressionFilter(ASTJexlScript script, TypeMetadata metadata, Set<String> nonEventFields) {
        this.nonEventFields = nonEventFields;
        AttributeFactory attributeFactory = new AttributeFactory(metadata);
        Map<String,EventDataQueryExpressionVisitor.ExpressionFilter> expressionFilters = EventDataQueryExpressionVisitor.getExpressionFilters(script,
                        attributeFactory);
        setFilters(expressionFilters);
    }
    
    public EventDataQueryExpressionFilter(JexlNode node, TypeMetadata metadata, Set<String> nonEventFields) {
        this.nonEventFields = nonEventFields;
        AttributeFactory attributeFactory = new AttributeFactory(metadata);
        Map<String,EventDataQueryExpressionVisitor.ExpressionFilter> expressionFilters = EventDataQueryExpressionVisitor.getExpressionFilters(node,
                        attributeFactory);
        setFilters(expressionFilters);
    }
    
    public EventDataQueryExpressionFilter(EventDataQueryExpressionFilter other) {
        this.nonEventFields = other.nonEventFields;
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
    
    protected void setFilters(Map<String,? extends PeekingPredicate<Key>> fieldFilters) {
        if (this.initialized) {
            throw new RuntimeException("This Projection instance was already initialized");
        }
        
        this.filters = Maps.newHashMap(fieldFilters);
        this.initialized = true;
    }
    
    protected Map<String,PeekingPredicate<Key>> getFilters() {
        return Collections.unmodifiableMap(this.filters);
    }
    
    @Override
    public boolean apply(Map.Entry<Key,String> input) {
        return apply(input.getKey(), true);
    }
    
    @Override
    public boolean peek(Map.Entry<Key,String> input) {
        return apply(input.getKey(), false);
    }
    
    public boolean peek(Key key) {
        return apply(key, false);
    }
    
    protected boolean apply(Key key, boolean update) {
        if (!this.initialized) {
            throw new RuntimeException("The EventDataQueryExpressionFilter was not initialized");
        }
        
        final DatawaveKey datawaveKey = new DatawaveKey(key);
        final String fieldName = JexlASTHelper.deconstructIdentifier(datawaveKey.getFieldName(), false);
        if (update) {
            return this.filters.containsKey(fieldName) && this.filters.get(fieldName).apply(key);
        } else {
            return this.filters.containsKey(fieldName) && this.filters.get(fieldName).peek(key);
        }
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
     * @return null
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
