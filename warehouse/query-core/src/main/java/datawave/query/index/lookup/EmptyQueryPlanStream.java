package datawave.query.index.lookup;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

import org.apache.commons.jexl3.parser.JexlNode;

import datawave.query.CloseableIterable;
import datawave.query.planner.QueryPlan;
import datawave.query.util.EmptyCloseableIterator;

/**
 * An empty {@link QueryPlanStream}
 */
public class EmptyQueryPlanStream implements QueryPlanStream {

    public static EmptyQueryPlanStream of() {
        return new EmptyQueryPlanStream();
    }

    @Override
    public CloseableIterable<QueryPlan> streamPlans(JexlNode node) {
        return new EmptyCloseableIterator<>();
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public Iterator<QueryPlan> iterator() {
        return Collections.emptyIterator();
    }
}
