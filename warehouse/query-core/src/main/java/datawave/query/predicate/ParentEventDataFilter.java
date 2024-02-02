package datawave.query.predicate;

import static datawave.query.jexl.visitors.EventDataQueryExpressionVisitor.ExpressionFilter;

import java.util.Map;

import org.apache.accumulo.core.data.Key;

/**
 * This filter will filter event data keys by only those fields that are required in the specified query.
 */
public class ParentEventDataFilter extends EventDataQueryExpressionFilter {

    /**
     * Preferred constructor
     *
     * @param filters
     *            a map of expression filters
     */
    public ParentEventDataFilter(Map<String,ExpressionFilter> filters) {
        super(filters);
    }

    public ParentEventDataFilter(ParentEventDataFilter other) {
        super(other);
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.query.function.Filter#keep(org.apache.accumulo.core.data.Key)
     */
    @Override
    public boolean keep(Key k) {
        // do not keep any of these fields because we will be re-fetching the parent document anyway
        return false;
    }

    @Override
    public EventDataQueryFilter clone() {
        return new ParentEventDataFilter(this);
    }
}
