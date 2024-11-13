package datawave.configuration;

/**
 * Wrapper around a configuration field which remembers if this field was prefixed or not.
 */
public class ConfField {

    private final boolean isPrefixed;
    private final String fieldName;

    public ConfField(String prefix, String fieldName) {
        this.isPrefixed = fieldName.startsWith(prefix + ".");
        this.fieldName = isPrefixed ? removePrefix(prefix, fieldName) : fieldName;
    }

    private final String removePrefix(String prefix, String key) {
        return key.replaceFirst(String.format("%s.", prefix), "");
    }

    /**
     * Get this field name. If this is a prefixed field, it will prepend the prefix.
     *
     * @param prefix
     *            a prefix
     * @return field name
     */
    public String getField(String prefix) {
        return isPrefixed ? String.format("%s.%s", prefix, fieldName) : fieldName;
    }

    /**
     * Get this field with no prefix.
     *
     * @return field name without any prefix.
     */
    public String getField() {
        return fieldName;
    }
}
