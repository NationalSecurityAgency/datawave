package datawave.query.function;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

/**
 * Represents a grouping-context field name of the form {@code BASE.GROUP.{intermediate}.INSTANCE}. For example, the field {@code CAT.PET.0} has base name
 * {@code CAT}, group {@code PET} and instance {@code 0}.
 * <p>
 * A field is considered grouped only when it has at least three dot-delimited tokens. When grouped, its {@link GroupAndInstance} identity is available via
 * {@link #getGroupAndInstance()}; otherwise that is null and {@link #isGrouped()} is false.
 * <p>
 * Construction via {@link #of(String)} does no parsing work - the {@link #getBaseName() base name} and the {@link #getGroupAndInstance() group and instance}
 * are computed lazily on first access and memoized thereafter, so callers on the hot path pay only for what they use. Instances are safe for concurrent use:
 * each memo is published through a single reference write to an immutable value ({@link String} or {@link GroupAndInstance}), which is safely publishable under
 * a data race. The only effect of a benign race is that two threads may redundantly compute an equal value.
 * <p>
 * Equality is over the full {@link #getName() field name}. To compare two fields as belonging to the same group instance (independent of their names), compare
 * their {@link GroupAndInstance} values instead - see {@link GroupAndInstance}.
 */
public final class FieldName {
    // Sentinel stored in the groupAndInstance memo to record that resolution has run and produced an ungrouped result. This lets us distinguish "not yet
    // computed" (null) from "computed, ungrouped" without a separate flag, keeping each memo a single reference write.
    private static final GroupAndInstance NOT_GROUPED = new GroupAndInstance("", "");

    private final String name;

    // Lazily computed memo fields. Each is written exactly once with an immutable value; the benign race on first access is sound (see class javadoc).
    private String baseName;
    private GroupAndInstance groupAndInstance;

    private FieldName(String name) {
        this.name = name;
    }

    /**
     * Create a field name wrapping the given name. This does no parsing; the base name and group/instance are resolved lazily on first access.
     *
     * @param field
     *            the field name
     * @return the field name; never null
     */
    public static FieldName of(String field) {
        return new FieldName(field);
    }

    /**
     * Return the base name of the given field - the portion before the first period - or the field itself when it has no grouping context.
     *
     * @param field
     *            the field name
     * @return the base name
     */
    public static String baseName(String field) {
        // Deliberately implemented with indexOf/substring rather than split("\\."): split strips trailing empty tokens, so an all-dot input such as "."
        // yields a length-0 array and a splits[0]-based implementation would throw ArrayIndexOutOfBoundsException, whereas this correctly returns "".
        int index = field.indexOf('.');
        return index != -1 ? field.substring(0, index) : field;
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
     * Get the base name of this field - the portion before the first period - or the full name when it has no grouping context. Computed lazily and memoized.
     *
     * @return the base name
     */
    public String getBaseName() {
        String result = baseName;
        if (result == null) {
            result = baseName(name);
            baseName = result;
        }
        return result;
    }

    /**
     * Return whether the field has a group and instance representation.
     *
     * @return true when the field is grouped, otherwise false
     */
    public boolean isGrouped() {
        return resolveGroupAndInstance() != NOT_GROUPED;
    }

    /**
     * Get the group and instance identity of this field, or null if the field is not grouped.
     *
     * @return the group and instance, or null when {@link #isGrouped()} is false
     */
    public GroupAndInstance getGroupAndInstance() {
        GroupAndInstance result = resolveGroupAndInstance();
        return result == NOT_GROUPED ? null : result;
    }

    /**
     * Resolve, and memoize on first access, the group and instance of this field. A field is grouped only when it has at least three dot-delimited tokens;
     * trailing empty tokens are ignored, so {@code "A.B.C."} resolves as the grouped field {@code A.B.C} with group {@code B} and instance {@code C}. The group
     * is the second token and the instance is the last token. Ungrouped fields resolve to the {@link #NOT_GROUPED} sentinel.
     *
     * @return the resolved group and instance, or the {@link #NOT_GROUPED} sentinel when ungrouped; never null
     */
    private GroupAndInstance resolveGroupAndInstance() {
        GroupAndInstance result = groupAndInstance;
        if (result == null) {
            String[] splits = name.split("\\.");
            if (splits.length >= 3) {
                // the group is the second token and the instance is the last token
                result = new GroupAndInstance(splits[1], splits[splits.length - 1]);
            } else {
                result = NOT_GROUPED;
            }
            groupAndInstance = result;
        }
        return result;
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
        return "FieldName{name=" + name + '}';
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
