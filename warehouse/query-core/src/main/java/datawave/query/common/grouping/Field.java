package datawave.query.common.grouping;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang.builder.ToStringBuilder;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;

/**
 * Represents an entry from a document with a field name broken down into its name, group, and instance, and the entry's attribute.
 */
class Field {

    private final String base;
    private final String groupingContext;
    private final String instance;
    private final Attribute<?> attribute;
    private final Set<Attribute<?>> attributes;

    public Field(String base, String groupingContext, String instance, Attribute<?> attribute) {
        this.base = base;
        this.groupingContext = groupingContext;
        this.instance = instance;
        this.attribute = attribute;

        if (attribute instanceof Attributes) {
            this.attributes = ((Attributes) attribute).getAttributes();
        } else {
            this.attributes = Collections.singleton(attribute);
        }
    }

    /**
     * Return the field base.
     *
     * @return the field base
     */
    public String getBase() {
        return base;
    }

    /**
     * Return whether this field has a grouping context as part of its name.
     *
     * @return true if this field has a group, or false otherwise
     */
    public boolean hasGroupingContext() {
        return groupingContext != null;
    }

    /**
     * Return the field's group, or null if the field does not have a group.
     *
     * @return the group
     */
    public String getGroupingContext() {
        return groupingContext;
    }

    /**
     * Return the field's instance, or null if the field does not have an instance.
     *
     * @return the instance
     */
    public String getInstance() {
        return instance;
    }

    /**
     * Return whether this field has an instance as part of its name.
     *
     * @return true if this field has an instance, or false otherwise
     */
    public boolean hasInstance() {
        return instance != null;
    }

    /**
     * Return this field's attribute
     *
     * @return the attribute
     */
    public Attribute<?> getAttribute() {
        return attribute;
    }

    /**
     * A convenience method for retrieving all attributes for this {@link Field}, particularly useful when dealing with a {@link Field} that was created with a
     * multi-value attribute. If the originating attribute was not multi-value, then the set will consist only of the same attribute returned by
     * {@link #getAttribute()}.
     *
     * @return all attributes, or same attribute as returned by {@link #getAttribute()} if the originating attribute was not multi-value
     */
    public Set<Attribute<?>> getAttributes() {
        return attributes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Field field = (Field) o;
        return Objects.equals(base, field.base) && Objects.equals(groupingContext, field.groupingContext) && Objects.equals(instance, field.instance)
                        && Objects.equals(attributes, field.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(base, groupingContext, instance, attributes);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("base", base).append("groupingContext", groupingContext).append("instance", instance)
                        .append("attributes", attributes).toString();
    }
}
