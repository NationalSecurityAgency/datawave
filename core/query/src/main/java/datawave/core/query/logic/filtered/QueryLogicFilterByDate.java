package datawave.core.query.logic.filtered;

import java.util.Set;

import org.apache.accumulo.core.security.Authorizations;

import datawave.core.query.predicate.QueryDateRangePredicate;
import datawave.microservice.query.Query;

/**
 * Tests that a Query's begin and end date overlap with the configured date range of the Predicate.
 */
public class QueryLogicFilterByDate extends QueryDateRangePredicate implements FilteredQueryLogic.QueryLogicFilter {
    /**
     * Test the Query settings against the QueryDateRangePredicate
     *
     * @param settings
     *            the settings including a beginDate and endDate
     * @param auths
     *            the query auths (unused for this test)
     * @return true if the query overlaps with the date range specified by the QueryDateRangePredicate, false otherwise
     */
    @Override
    public boolean canRunQuery(Query settings, Set<Authorizations> auths) {
        return test(settings);
    }
}
