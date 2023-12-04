package datawave.webservice.query.predicate;

import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;

import java.util.function.Predicate;

/**
 * This is a predicate that will test a specified query parameter matches a specified value. If no value is specified then the parameter is treated as a boolean
 * parameter (i.e. the value is "true").
 */
public class QueryParameterPredicate implements Predicate<Query> {
    
    // The parameter to match against
    private String parameter;
    
    // The value to match against. If empty then boolean value of parameter is used.
    private String value;
    
    public QueryParameterPredicate() {}
    
    public QueryParameterPredicate(String parameter) {
        setParameter(parameter);
    }
    
    public QueryParameterPredicate(String parameter, String value) {
        this(parameter);
        setValue(value);
    }
    
    @Override
    public boolean test(Query settings) {
        return matches(settings.findParameter(getParameter()));
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
    
    @Override
    public String toString() {
        return "(parameter " + parameter + " == " + value + ')';
    }
}
