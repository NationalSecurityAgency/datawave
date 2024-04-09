package datawave.core.query.jexl;

import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * The values of T should already be normalized
 *
 *
 *
 * @param <T>
 *            type of the range
 */
public class LiteralRange<T extends Comparable<T>> implements Comparable<LiteralRange<T>> {

    public enum NodeOperand {
        OR, AND
    }

    private String fieldName;
    private T lower, upper;
    private NodeOperand operand;
    private Boolean lowerInclusive, upperInclusive;
    private JexlNode lowerNode, upperNode;

    public LiteralRange(String fieldName, NodeOperand operand) {
        this.fieldName = fieldName;
        this.operand = operand;
    }

    public LiteralRange(T lower, Boolean lowerInclusive, T upper, Boolean upperInclusive, String fieldName, NodeOperand operand) {
        this(fieldName, operand);
        this.lower = lower;
        this.lowerInclusive = lowerInclusive;
        this.upper = upper;
        this.upperInclusive = upperInclusive;
    }

    public String getFieldName() {
        return fieldName;
    }

    public T getLower() {
        return lower;
    }

    private void setLower(T lower) {
        this.lower = lower;
    }

    public JexlNode getLowerNode() {
        return lowerNode;
    }

    public Boolean isLowerInclusive() {
        return lowerInclusive;
    }

    private void setLowerInclusive(Boolean lowerInclusive) {
        this.lowerInclusive = lowerInclusive;
    }

    public void updateLower(T candidateLower, Boolean candidateInclusive, JexlNode node) {
        if (null == lower) {
            this.lowerNode = node;
            this.lower = candidateLower;
            this.lowerInclusive = candidateInclusive;
        } else {
            int cmp = lower.compareTo(candidateLower);

            if (operand.equals(NodeOperand.AND)) {
                if (cmp < 0 || (cmp == 0 && this.lowerInclusive && !candidateInclusive)) {
                    this.lowerInclusive = candidateInclusive;
                    this.lower = candidateLower;
                    this.lowerNode = node;
                }
            } else {
                if (cmp > 0 || (cmp == 0 && !this.lowerInclusive && candidateInclusive)) {
                    this.lowerInclusive = candidateInclusive;
                    this.lower = candidateLower;
                    this.lowerNode = node;
                }
            }
        }
    }

    public T getUpper() {
        return upper;
    }

    private void setUpper(T upper) {
        this.upper = upper;
    }

    public JexlNode getUpperNode() {
        return upperNode;
    }

    public Boolean isUpperInclusive() {
        return upperInclusive;
    }

    private void setUpperInclusive(Boolean upperInclusive) {
        this.upperInclusive = upperInclusive;
    }

    public NodeOperand getNodeOperand() {
        return this.operand;
    }

    public void updateUpper(T candidateUpper, Boolean candidateInclusive, JexlNode node) {
        if (null == upper) {
            this.upperNode = node;
            this.upper = candidateUpper;
            this.upperInclusive = candidateInclusive;
        } else {
            int cmp = upper.compareTo(candidateUpper);

            if (operand.equals(NodeOperand.AND)) {
                if (cmp > 0 || (cmp == 0 && this.upperInclusive && !candidateInclusive)) {
                    this.upperInclusive = candidateInclusive;
                    this.upper = candidateUpper;
                    this.upperNode = node;
                }
            } else {
                if (cmp < 0 || (cmp == 0 && !this.upperInclusive && candidateInclusive)) {
                    this.upperInclusive = candidateInclusive;
                    this.upper = candidateUpper;
                    this.upperNode = node;
                }
            }
        }
    }

    public boolean contains(T value) {
        boolean matches = true;
        if (isLowerInclusive()) {
            matches = (getLower().compareTo(value) <= 0);
        } else {
            matches = (getLower().compareTo(value) < 0);
        }
        if (matches) {
            if (isUpperInclusive()) {
                matches = (value.compareTo(getUpper()) <= 0);
            } else {
                matches = (value.compareTo(getUpper()) < 0);
            }
        }
        return matches;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append(this.fieldName).append(":");

        if (null != this.lowerInclusive) {
            if (this.lowerInclusive) {
                sb.append("[");
            } else {
                sb.append("(");
            }
        } else {
            sb.append("(");
        }

        if (null == this.lower) {
            sb.append("-inf");
        } else {
            sb.append(this.lower);
        }

        sb.append(", ");

        if (null == this.upper) {
            sb.append("+inf");
        } else {
            sb.append(this.upper);
        }

        if (null != this.upperInclusive) {
            if (this.upperInclusive) {
                sb.append("]");
            } else {
                sb.append(")");
            }
        } else {
            sb.append(")");
        }

        return sb.toString();
    }

    public boolean isBounded() {
        return this.lower != null && this.upper != null && this.fieldName != null;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(fieldName).append(lower).append(upper).append(operand).append(lowerInclusive).append(upperInclusive).toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof LiteralRange) {
            LiteralRange<?> o = (LiteralRange<?>) obj;
            return new EqualsBuilder().append(fieldName, o.fieldName).append(lower, o.lower).append(upper, o.upper).append(operand, o.operand)
                            .append(lowerInclusive, o.lowerInclusive).append(upperInclusive, o.upperInclusive).isEquals();
        }
        return false;
    }

    @Override
    public int compareTo(LiteralRange<T> o) {
        return new CompareToBuilder().append(fieldName, o.fieldName).append(lower, o.lower).append(upper, o.upper).append(operand, o.operand)
                        .append(lowerInclusive, o.lowerInclusive).append(upperInclusive, o.upperInclusive).toComparison();
    }

}
