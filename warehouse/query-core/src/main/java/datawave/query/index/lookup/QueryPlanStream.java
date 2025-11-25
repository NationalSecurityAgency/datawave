package datawave.query.index.lookup;

import org.apache.commons.jexl3.parser.JexlNode;

import datawave.query.CloseableIterable;
import datawave.query.planner.QueryPlan;

/**
 * Base interface used by {@link RangeStream} and others.
 */
public interface QueryPlanStream extends CloseableIterable<QueryPlan> {

    /**
     * Creates a closable stream of QueryPlans
     *
     * @param node
     *            the query tree
     * @return a closeable iterable of QueryPlans
     */
    CloseableIterable<QueryPlan> streamPlans(JexlNode node);
}
