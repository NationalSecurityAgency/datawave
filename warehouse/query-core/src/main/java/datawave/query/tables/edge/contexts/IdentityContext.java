package datawave.query.tables.edge.contexts;

import datawave.edge.model.EdgeModelFields;
import org.apache.commons.lang.StringUtils;

public class IdentityContext implements EdgeContext {
    private EdgeModelFields.FieldKey identity;
    private String literal;
    private String operation;
    boolean equivalence;

    public IdentityContext(String internalFieldName, String literal, String opp, EdgeModelFields fields) {
        this.identity = fields.parse(internalFieldName);
        this.literal = literal;
        this.operation = opp;

        if (opp.equals(EdgeModelFields.NOT_EQUALS) || opp.equals(EdgeModelFields.NOT_EQUALS_REGEX)) {
            equivalence = false;
        } else {
            equivalence = true;
        }
    }

    public String getEscapedLiteral() {
        if (literal == null || literal.equals("")) {
            throw new IllegalArgumentException("Null/empty values are not allowed.");
        }
        // don't need to escape double quotes because the query string we send down to edge filter iterator will always be wrapped in single quotes

        return StringUtils.replace(literal, "'", "\\'");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof IdentityContext))
            return false;

        IdentityContext that = (IdentityContext) o;

        if (equivalence != that.equivalence)
            return false;
        if (identity != null ? !identity.equals(that.identity) : that.identity != null)
            return false;
        if (literal != null ? !literal.equals(that.literal) : that.literal != null)
            return false;
        return !(operation != null ? !operation.equals(that.operation) : that.operation != null);

    }

    @Override
    public int hashCode() {
        int result = identity != null ? identity.hashCode() : 0;
        result = 31 * result + (literal != null ? literal.hashCode() : 0);
        result = 31 * result + (operation != null ? operation.hashCode() : 0);
        result = 31 * result + (equivalence ? 1 : 0);
        return result;
    }

    public EdgeModelFields.FieldKey getIdentity() {
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
