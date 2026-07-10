package datawave.query.function;

import java.util.Objects;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

/**
 * Represents a parsed grouping-context field name of the form {@code BASE.GROUP.{intermediate}.INSTANCE}. For example, the field {@code CAT.PET.0} has group
 * {@code PET} and instance {@code 0}.
 * <p>
 * A field is considered grouped only when it has at least three dot-delimited tokens. When grouped, its {@link GroupAndInstance} identity is available via
 * {@link #getGroupAndInstance()}; otherwise that is null and {@link #isGrouped()} is false.
 * <p>
 * Equality is over the full {@link #getName() field name}. To compare two fields as belonging to the same group instance (independent of their names), compare
 * their {@link GroupAndInstance} values instead - see {@link GroupAndInstance}.
 */
public final class FieldName {
    private final String name;
    private final GroupAndInstance groupAndInstance;

    private FieldName(String name, GroupAndInstance groupAndInstance) {
        this.name = name;
        this.groupAndInstance = groupAndInstance;
    }

    /**
     * Parse the given field name into its base, group, and instance. A field is grouped only when it has at least three dot-delimited tokens; trailing empty
     * tokens are ignored, so {@code "A.B.C."} parses as the grouped field {@code A.B.C} with group {@code B} and instance {@code C}.
     *
     * @param field
     *            the field name to parse
     * @return the parsed field name; never null
     */
    public static FieldName parse(String field) {
        String[] splits = field.split("\\.");
        if (splits.length >= 3) {
            // the group is the second token and the instance is the last token
            return new FieldName(field, new GroupAndInstance(splits[1], splits[splits.length - 1]));
        }
        return new FieldName(field, null);
    }

    /**
     * Get the full field name.
     *
     * @return the field name
     */
    public String getName() {
        return name;
    }

    /**
     * Return whether the field has a group and instance representation.
     *
     * @return true when the field is grouped, otherwise false
     */
    public boolean isGrouped() {
        return groupAndInstance != null;
    }

    /**
     * Get the group and instance identity of this field, or null if the field is not grouped.
     *
     * @return the group and instance, or null when {@link #isGrouped()} is false
     */
    public GroupAndInstance getGroupAndInstance() {
        return groupAndInstance;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof FieldName))
            return false;
        return name.equals(((FieldName) o).name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }

    /**
     * The group and instance identity parsed from a grouping-context field name, independent of the field name it came from. Two instances are equal when their
     * group and instance match, so a field such as {@code AGE.PERSON.1} matches {@code NAME.PERSON.1} as belonging to the same group instance.
     */
    public static final class GroupAndInstance {
        private final String group;
        private final String instance;

        GroupAndInstance(String group, String instance) {
            this.group = group;
            this.instance = instance;
        }

        /**
         * The group token (a.k.a the commonality token) in the field.
         *
         * @return the group
         */
        public String getGroup() {
            return group;
        }

        /**
         * The instance token in the field.
         *
         * @return the instance
         */
        public String getInstance() {
            return instance;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (!(o instanceof GroupAndInstance))
                return false;

            GroupAndInstance that = (GroupAndInstance) o;
            return group.equals(that.group) && instance.equals(that.instance);
        }

        @Override
        public int hashCode() {
            int result = group.hashCode();
            result = 31 * result + instance.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return ReflectionToStringBuilder.toString(this);
        }
    }
}
