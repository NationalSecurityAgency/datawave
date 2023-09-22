package datawave.webservice.query.logic.filtered;

import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.security.Authorizations;

import datawave.microservice.query.Query;
import datawave.webservice.query.predicate.ProxiedAuthorizationsPredicate;

/**
 * This is a filter for the FilteredQueryLogic that will run the delegate query logic if the auths requested match a specified visibility (as defined by
 * accumulo's ColumnVisibility). In addition to the visibility, one can specify that only the first proxied user is matched (presumably the user), and one can
 * negate the matching of the visibility.
 */
public class QueryLogicFilterByAuth extends ProxiedAuthorizationsPredicate implements FilteredQueryLogic.QueryLogicFilter {

    // if negated than the negation of the match is returned
    private boolean negated = false;

    public QueryLogicFilterByAuth() {}

    public QueryLogicFilterByAuth(String visibility) {
        setVisibility(visibility);
    }

    public QueryLogicFilterByAuth(String visibility, MatchType matchType) {
        this(visibility);
        setMatchType(matchType);
    }

    public QueryLogicFilterByAuth(String visibility, MatchType matchType, boolean negated) {
        this(visibility, matchType);
        setNegated(negated);
    }

    @Override
    public boolean canRunQuery(Query settings, Set<Authorizations> auths) {
        boolean canRunQuery = test(auths.stream().collect(Collectors.toList()));
        if (negated) {
            canRunQuery = !canRunQuery;
        }
        return canRunQuery;
    }

    public boolean isNegated() {
        return negated;
    }

    public void setNegated(boolean negated) {
        this.negated = negated;
    }
}
