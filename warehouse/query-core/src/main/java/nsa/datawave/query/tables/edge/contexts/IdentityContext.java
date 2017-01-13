package nsa.datawave.query.tables.edge.contexts;

import nsa.datawave.edge.model.EdgeModelAware;
import org.apache.commons.lang3.StringUtils;

public class IdentityContext implements EdgeModelAware, EdgeContext {
    private String identity;
    private String literal;
    private String operation;
    boolean equivalence;
    
    public IdentityContext(String identity, String literal, String opp) {
        this.identity = identity;
        this.literal = literal;
        this.operation = opp;
        
        if (opp.equals(NOT_EQUALS) || opp.equals(NOT_EQUALS_REGEX)) {
            equivalence = false;
        } else {
            equivalence = true;
        }
    }
    
    public String getEscapedLiteral() {
        if (literal == null || literal.equals("")) {
            throw new IllegalArgumentException("Null/empty values are not allowed.");
        }
        String escapedString = StringUtils.replace(literal, "'", "\\'");
        // don't need to escape double quotes because the query string we send down to edge filter iterator will always be wrapped in single quotes
        
        return escapedString;
    }
    
    public String getIdentity() {
        return identity;
    }
    
    public String getLiteral() {
        return literal;
    }
    
    public String getOperation() {
        return operation;
    }
    
    public boolean isEquivalence() {
        return equivalence;
    }
}
