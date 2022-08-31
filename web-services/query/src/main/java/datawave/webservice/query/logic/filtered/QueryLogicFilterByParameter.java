package datawave.webservice.query.logic.filtered;

import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import org.apache.accumulo.core.security.Authorizations;

import java.util.Set;

public class QueryLogicFilterByParameter implements FilteredQueryLogic.QueryLogicFilter {
    
    // The parameter to match against
    private String parameter;
    
    // The value to match against. If empty then boolean value of parameter is used.
    private String value;
    
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
        boolean canRunQuery = matches(settings.findParameter(getParameter()));
        if (negated) {
            canRunQuery = !canRunQuery;
        }
        return canRunQuery;
    }
    
    private boolean matches(QueryImpl.Parameter parameter) {
        String parameterValue = (parameter == null ? null : parameter.getParameterValue());
        if (value == null) {
            return Boolean.valueOf(parameterValue);
        } else {
            return value.equals(parameterValue);
        }
    }
    
    public String getParameter() {
        return parameter;
    }
    
    public void setParameter(String parameter) {
        this.parameter = parameter;
    }
    
    public String getValue() {
        return value;
    }
    
    public void setValue(String value) {
        this.value = value;
    }
    
    public boolean isNegated() {
        return negated;
    }
    
    public void setNegated(boolean negated) {
        this.negated = negated;
    }
}
