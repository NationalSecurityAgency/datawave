package datawave.query.jexl.visitors;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * There may exist certain field-pattern combinations that you always want to rewrite
 */
public class RegexRewritePattern {
    private String field;
    private String literal;

    public RegexRewritePattern(String field, String literal) {
        this.field = field;
        this.literal = literal;
    }

    public boolean matches(String field, String literal) {
        return this.field.equals(field) && this.literal.equals(literal);
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getLiteral() {
        return literal;
    }

    public void setLiteral(String literal) {
        this.literal = literal;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof RegexRewritePattern) {
            RegexRewritePattern other = (RegexRewritePattern) o;
            return new EqualsBuilder().append(field, other.field).append(literal, other.literal).isEquals();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(field).append(literal).hashCode();
    }
}
