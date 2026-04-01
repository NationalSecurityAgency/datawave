package datawave.query.planner.rules;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class FieldPattern {
    private String field;
    private String pattern;

    public FieldPattern() {
        // empty constructor
    }

    public FieldPattern(String field, String pattern) {
        this.field = field;
        this.pattern = pattern;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getPattern() {
        return pattern;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        FieldPattern that = (FieldPattern) o;

        return new EqualsBuilder().append(field, that.field).append(pattern, that.pattern).isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(field).append(pattern).toHashCode();
    }
}
