package datawave.query.predicate;

import java.util.Map;

import org.apache.accumulo.core.data.Key;

import datawave.query.jexl.visitors.EventDataQueryExpressionVisitor;
import datawave.query.jexl.visitors.EventDataQueryExpressionVisitor.ExpressionFilter;

/**
 * A data filter that operates on TermFrequency keys
 */
public class TermFrequencyDataFilter extends EventDataQueryExpressionFilter {

    /**
     * Constructor matching {@link EventDataQueryExpressionVisitor}
     *
     * @param filters
     *            a map of expression filters
     */
    public TermFrequencyDataFilter(Map<String,ExpressionFilter> filters) {
        super(filters);
    }

    /**
     * TermFrequency ranges necessitate a full value match
     *
     * @param key
     *            a term frequency key
     * @return the same value as {@link #peek(Key)}
     */
    @Override
    public boolean keep(Key key) {
        // for things that will otherwise be added need to ensure it's actually a value match. This is necessary when dealing with TF ranges.
        return peek(key);
    }
}
