package datawave.query.predicate;

import java.util.Collections;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;

import com.google.common.collect.Maps;

import datawave.query.data.parsers.DatawaveKey;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.EventDataQueryExpressionVisitor.ExpressionFilter;

/**
 * This class is used to filter out fields that are required for evaluation by apply the query expressions to the field values on the fly. This filter will
 * "keep" all of those returned by "apply". If more fields are required to be returned to the user, then this class must be overridden. startNewDocument will be
 * called with a documentKey whenever we are starting to scan a new document or document tree.
 */
public class EventDataQueryExpressionFilter implements EventDataQueryFilter {
    private Map<String,ExpressionFilter> filters = null;
    private boolean initialized = false;

    protected Key document = null;

    /**
     * Preferred constructor
     *
     * @param filters
     *            a prebuilt map of expression filters
     */
    public EventDataQueryExpressionFilter(Map<String,ExpressionFilter> filters) {
        setFilters(filters);
    }

    public EventDataQueryExpressionFilter(EventDataQueryExpressionFilter other) {
        setFilters(ExpressionFilter.clone(other.getFilters()));
        if (other.document != null) {
            document = new Key(other.document);
        }
    }

    @Override
    public void startNewDocument(Key document) {
        this.document = document;
        // since we are starting a new document, reset the filters
        ExpressionFilter.reset(filters);
    }

    @Override
    public boolean keep(Key k) {
        return true;
    }

    /**
     * Sets a map of expression filters, throws an exception if this is called multiple times
     *
     * @param fieldFilters
     *            a map of expression filters
     */
    protected void setFilters(Map<String,ExpressionFilter> fieldFilters) {
        if (this.initialized) {
            throw new RuntimeException("This Projection instance was already initialized");
        }

        this.filters = Maps.newHashMap(fieldFilters);
        this.initialized = true;
    }

    protected Map<String,ExpressionFilter> getFilters() {
        return Collections.unmodifiableMap(this.filters);
    }

    @Override
    public boolean apply(Map.Entry<Key,String> entry) {
        return apply(entry.getKey(), true);
    }

    @Override
    public boolean peek(Map.Entry<Key,String> entry) {
        return apply(entry.getKey(), false);
    }

    public boolean peek(Key key) {
        return apply(key, false);
    }

    protected boolean apply(Key key, boolean update) {
        if (!this.initialized) {
            throw new RuntimeException("The EventDataQueryExpressionFilter was not initialized");
        }

        final DatawaveKey datawaveKey = new DatawaveKey(key);
        // FIELD_NAME.0
        // FIELD_NAME.1 = deconstruct identifier = remove dot number from end of field name
        final String fieldName = JexlASTHelper.deconstructIdentifier(datawaveKey.getFieldName(), false);
        final String fieldValue = datawaveKey.getFieldValue();
        if (update) {
            return this.filters.containsKey(fieldName) && this.filters.get(fieldName).apply(key, fieldName, fieldValue);
        } else {
            return this.filters.containsKey(fieldName) && this.filters.get(fieldName).peek(key, fieldName, fieldValue);
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
