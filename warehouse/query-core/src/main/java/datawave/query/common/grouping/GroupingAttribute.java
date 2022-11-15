package datawave.query.common.grouping;

import datawave.data.type.Type;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.TypeAttribute;
import org.apache.accumulo.core.data.Key;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * This class serves as a wrapper for the {@link TypeAttribute} that overrides the default {@code equals()} and {@code hashCode()} behavior so that equality is
 * determined by the attribute's field and value, and the hashCode is generated solely with the attribute's value.
 *
 * @param <T>
 *            the delegate type
 */
public class GroupingAttribute<T extends Comparable<T>> extends TypeAttribute<T> {
    
    public GroupingAttribute(Type<T> type, Key key, boolean toKeep) {
        super(type, key, toKeep);
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
        if (other instanceof TypeAttribute<?>) {
            TypeAttribute<?> otherType = (TypeAttribute<?>) other;
            return this.getType().equals(otherType.getType()) && isMetadataRowEqual(otherType);
        }
        return false;
    }
    
    // Return whether the field is equal.
    private boolean isMetadataRowEqual(Attribute<?> other) {
        if (this.isMetadataSet() == other.isMetadataSet()) {
            if (this.isMetadataSet()) {
                return this.metadata.compareRow(other.getMetadata().getRow()) == 0;
            }
        }
        return false;
    }
    
    /**
     * Returns a hashcode of the attribute's value.
     */
    @Override
    public int hashCode() {
        return new HashCodeBuilder(2099, 2129).append(getType().getDelegateAsString()).toHashCode();
    }
}
