package datawave.webservice.query.logic.filtered;

import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.predicate.QueryParameterPredicate;
import org.apache.accumulo.core.security.Authorizations;

import java.util.Set;

/**
 * This is a filter for the FilteredQueryLogic that will run the delegate query logic if a specified query parameter matches a specified value. If no value is
 * specified then the parameter is treated as a boolean parameter (i.e. the value is "true"). One can also negate the matching of the parameter.
 */
public class QueryLogicFilterByParameter extends QueryParameterPredicate implements FilteredQueryLogic.QueryLogicFilter {
    
    // if negated than the negation of the match is returned
    private boolean negated = false;
    
    public QueryLogicFilterByParameter() {}
    
    public QueryLogicFilterByParameter(String parameter) {
        setParameter(parameter);
    }
    
    public QueryLogicFilterByParameter(String parameter, String value) {
        this(parameter);
        setValue(value);
    }
    
    public QueryLogicFilterByParameter(String parameter, boolean negated) {
        this(parameter);
        setNegated(negated);
    }
    
    public QueryLogicFilterByParameter(String parameter, String value, boolean negated) {
        this(parameter, value);
        setNegated(negated);
    }
    
    @Override
    public boolean canRunQuery(Query settings, Set<Authorizations> auths) {
        boolean canRunQuery = test(settings);
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
    
    @Override
    public String toString() {
        return '(' + super.toString() + " == " + !negated + ')';
    }
}
