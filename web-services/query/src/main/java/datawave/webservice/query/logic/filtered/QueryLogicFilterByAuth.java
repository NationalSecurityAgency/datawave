package datawave.webservice.query.logic.filtered;

import datawave.webservice.query.Query;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.VisibilityEvaluator;
import org.apache.accumulo.core.security.VisibilityParseException;

import java.util.Set;

public class QueryLogicFilterByAuth implements FilteredQueryLogic.QueryLogicFilter {
    
    public static enum MatchType {
        // match only the first of the Authorizations (presumably the user)
        FIRST,
        // match all of the Authorizations (the user and proxies)
        ALL,
    }
    
    // A visibility string to be matched against the auths being used for the query
    private ColumnVisibility visibility;
    
    // Default is to only require the user visibilities to match
    private MatchType matchType = MatchType.ALL;
    
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
        boolean canRunQuery = matches(auths);
        if (negated) {
            canRunQuery = !canRunQuery;
        }
        return canRunQuery;
    }
    
    private boolean matches(Set<Authorizations> auths) {
        // match the visibility against the auths. If any of the authorizations to not match
        ColumnVisibility vis = getVisibility();
        for (Authorizations auth : auths) {
            VisibilityEvaluator ve = new VisibilityEvaluator(auth);
            try {
                if (ve.evaluate(vis)) {
                    if (matchType == MatchType.FIRST) {
                        return true;
                    }
                } else {
                    return false;
                }
            } catch (VisibilityParseException e) {
                throw new RuntimeException(e);
            }
        }
        return true;
    }
    
    public ColumnVisibility getVisibility() {
        return visibility;
    }
    
    public void setVisibility(ColumnVisibility visibility) {
        this.visibility = visibility;
    }
    
    public void setVisibility(String visibility) {
        setVisibility(new ColumnVisibility(visibility));
    }
    
    public MatchType getMatchType() {
        return matchType;
    }
    
    public void setMatchType(MatchType matchType) {
        this.matchType = matchType;
    }
    
    public boolean isNegated() {
        return negated;
    }
    
    public void setNegated(boolean negated) {
        this.negated = negated;
    }
}
