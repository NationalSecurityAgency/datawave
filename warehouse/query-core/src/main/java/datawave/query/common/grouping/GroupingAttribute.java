package datawave.query.common.grouping;

import java.util.Objects;

import org.apache.accumulo.core.data.Key;
import org.apache.commons.lang.builder.HashCodeBuilder;

import datawave.data.type.Type;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.TypeAttribute;

/**
 * This class serves as a wrapper for the {@link TypeAttribute} that overrides the default {@link #equals(Object)} and {@link #hashCode()} behavior so that
 * equality is determined by the attribute's field and value, and the hashCode is generated solely with the attribute's value.
 *
 * @param <T>
 *            the delegate type
 */
@SuppressWarnings("rawtypes")
public class GroupingAttribute<T extends Comparable<T>> extends TypeAttribute<T> {

    /**
     * This type, if not null, will be used instead of the original type when comparing types in the {@link #equals(Object)} and {@link #hashCode()}. This
     * allows us to preserve the original type value while also comparing based on a transformed version of the value, such as when applying a temporal
     * granularity in the case of {@code #GROUPBY(FOO[DAY])}.
     */
    private final String overridingValue;

    public GroupingAttribute(Type<T> type, Key key, boolean toKeep) {
        this(type, key, toKeep, null);
    }

    public GroupingAttribute(Type<T> type, Key key, boolean toKeep, String overridingValue) {
        super(type, key, toKeep);
        this.overridingValue = overridingValue;
    }

    public String getOverridingValue() {
        return overridingValue;
    }

    public boolean hasOverridingValue() {
        return overridingValue != null;
    }

    /**
     * Returns whether the other attribute has the same field and value.
     *
     * @param other
     *            the other attribute
     * @return true if the attribute is considered equal, or false otherwise
     */
    @Override
    public boolean equals(Object other) {
        if (null == other) {
            return false;
        }
        if (other instanceof GroupingAttribute<?>) {
            GroupingAttribute<?> otherType = (GroupingAttribute<?>) other;
            // If either attribute has a type override, determine equality based on the class of the types, the overriding value, and the metadata row. This
            // allows us to make groupings that may involve versions of a field value that has been transformed such as for #GROUPBY(FIELD[DAY]).
            if (this.hasOverridingValue() || otherType.hasOverridingValue()) {
                // @formatter:off
                return Objects.equals(this.getType().getClass(), otherType.getType().getClass()) &&
                                Objects.equals(this.overridingValue, otherType.overridingValue) &&
                                isMetadataRowEqual(otherType);
                // @formatter:on
            } else {
                // If neither attribute has a comparing type override
                return this.getType().equals(otherType.getType()) && isMetadataRowEqual(otherType);
            }

        }
        return false;
    }

    /**
     * Return whether the metadata row of this attribute is considered equal to the row of the other attribute.
     *
     * @param other
     *            the other attribute
     * @return true if the metadata row is equal, or false otherwise
     */
    private boolean isMetadataRowEqual(Attribute<?> other) {
        return this.isMetadataSet() == other.isMetadataSet() && (!this.isMetadataSet() || (this.getMetadata().getRow().equals(other.getMetadata().getRow())));
    }

    /**
     * Returns the hashcode of the attribute's value.
     *
     * @return the hashcode of the attribute's value
     */
    @Override
    public int hashCode() {
        return hasOverridingValue() ? hashCodeOf(overridingValue) : hashCodeOf(getType().getDelegateAsString());
    }

    private int hashCodeOf(String value) {
        return new HashCodeBuilder(2099, 2129).append(value).toHashCode();
    }
}
