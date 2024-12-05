package datawave.core.query.logic.filtered;

import java.util.Set;

import org.apache.accumulo.core.security.Authorizations;

import datawave.core.query.predicate.QueryDateRangePredicate;
import datawave.microservice.query.Query;

public class QueryLogicFilterByDate extends QueryDateRangePredicate implements FilteredQueryLogic.QueryLogicFilter {
    @Override
    public boolean canRunQuery(Query settings, Set<Authorizations> auths) {
        return test(settings);
    }
}
