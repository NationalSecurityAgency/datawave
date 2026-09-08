package datawave.security.utils;

/**
 * Utility class for parsing configuration options.
 */
public final class ConfigUtils {

    /**
     * Return the trimmed value if it is non-blank, or the default value otherwise.
     *
     * @param value
     *            the value
     * @param defaultValue
     *            the default value
     * @return the string
     */
    public static String getString(String value, String defaultValue) {
        if (value != null && !value.isBlank()) {
            return value.trim();
        } else {
            return defaultValue;
        }
    }

    /**
     * Return the given value as a boolean if it is non-blank, or the default value otherwise
     *
     * @param value
     *            the value
     * @param defaultValue
     *            the default value
     * @return the boolean
     */
    public static Boolean getBoolean(String value, boolean defaultValue) {
        if (value != null && !value.isBlank()) {
            return Boolean.valueOf(value.trim());
        } else {
            return defaultValue;
        }
    }

    /**
     * Return the given value as a long if it is non-blank, or the default value otherwise.
     *
     * @param value
     *            the value
     * @param defaultValue
     *            the default value
     * @return the long
     */
    public static Long getLong(String value, long defaultValue) {
        if (value != null && !value.isBlank()) {
            return Long.valueOf(value.trim());
        } else {
            return defaultValue;
        }
    }

    private ConfigUtils() {
        throw new UnsupportedOperationException();
    }
}
