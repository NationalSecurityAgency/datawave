package datawave.ingest.data.config;

import org.apache.accumulo.core.security.ColumnVisibility;

import datawave.visibility.AccessExpressionUtil;

/**
 * Helpers for working with the per-field visibility carried by a {@link NormalizedContentInterface}.
 * <p>
 * A field visibility that is null or empty means "use the event visibility". It is never treated as an empty (public) visibility.
 */
public final class FieldVisibilities {

    private FieldVisibilities() {}

    /**
     * @param visibility
     *            the visibility to check
     * @return true if the visibility is null or has an empty expression
     */
    public static boolean isEmpty(ColumnVisibility visibility) {
        return visibility == null || visibility.getExpression().length == 0;
    }

    /**
     * Get the field specific visibility of a value in normalized form.
     * <p>
     * The normalization is the same one previously applied when converting field markings to a column visibility, so the flattened bytes written to Accumulo do
     * not change.
     *
     * @param value
     *            the field value, may be null
     * @return the normalized field visibility, or null if the value has no field specific visibility and the event visibility should be used
     */
    public static ColumnVisibility getFieldVisibility(NormalizedContentInterface value) {
        if (value == null) {
            return null;
        }
        ColumnVisibility visibility = value.getVisibility();
        if (isEmpty(visibility)) {
            return null;
        }
        return AccessExpressionUtil.toColumnVisibility(AccessExpressionUtil.normalize(visibility));
    }

    /**
     * Resolve the visibility to use for a field: the normalized field specific visibility if present, otherwise the event visibility.
     *
     * @param eventVisibility
     *            the event visibility, may be null
     * @param value
     *            the field value, may be null
     * @return the visibility for the field; null only if neither the field nor the event has a visibility
     */
    public static ColumnVisibility resolve(ColumnVisibility eventVisibility, NormalizedContentInterface value) {
        ColumnVisibility fieldVisibility = getFieldVisibility(value);
        return fieldVisibility != null ? fieldVisibility : eventVisibility;
    }
}
