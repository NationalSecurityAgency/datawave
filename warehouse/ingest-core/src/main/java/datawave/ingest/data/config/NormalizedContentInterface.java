package datawave.ingest.data.config;

import org.apache.accumulo.core.security.ColumnVisibility;

import datawave.data.type.Type;
import datawave.marking.AccessExpressionMarkings;
import datawave.marking.Markings;

/**
 * Interface utilized to reference the original and transformed content and labels for a value pair ingested into accumulo.
 */
public interface NormalizedContentInterface extends Cloneable {
    /**
     * Sets the field name, normally returned by both getIndexedFieldName and getEventFieldName
     *
     * @param name
     *            the field name
     */
    void setFieldName(String name);

    /**
     * Gets the field name. Normally this is the same as getEventFieldName.
     *
     * @return the field name
     */
    String getIndexedFieldName();

    /**
     * Gets the event field name. Normally this is the same as getIndexedFieldName. but could be different
     *
     * @return the event field name
     */
    String getEventFieldName();

    /**
     * Get the event field value
     *
     * @return the field value
     */
    String getEventFieldValue();

    /**
     * Set the event field value
     *
     * @param val
     *            the event field value
     */
    void setEventFieldValue(String val);

    /**
     * Gets for the indexed field value
     *
     * @return the indexed field value
     */
    String getIndexedFieldValue();

    /**
     * Sets the indexed field value
     *
     * @param val
     *            the indexed field value
     */
    void setIndexedFieldValue(String val);

    /**
     * Set the field specific visibility.
     *
     * @param visibility
     *            the visibility for this field, or null if the overall event visibility is to be used
     */
    void setVisibility(ColumnVisibility visibility);

    /**
     * Get the field specific visibility.
     *
     * @return The field specific visibility, or null if the overall event visibility is to be used. An empty visibility is treated the same as null by the
     *         ingest handlers (see {@link FieldVisibilities}).
     */
    ColumnVisibility getVisibility();

    /**
     * Set the field specific markings
     *
     * @param markings
     *            the markings to set
     * @deprecated use {@link #setVisibility(ColumnVisibility)}
     */
    // TODO remove-accumulo-utils
    @Deprecated(forRemoval = true)
    default void setMarkings(Markings<?> markings) {
        setVisibility(markings == null || markings.isEmpty() ? null : markings.toColumnVisibility());
    }

    /**
     * When we fail to process a field (e.g. normalize), then an error can be set which in the EventMapper results in the event being dropped from normal
     * processing and sent over to the ErrorDataTypeHandler.
     *
     * @param e
     *            the error to throw
     */
    void setError(Throwable e);

    /**
     * Get the field specific markings
     *
     * @return The field specific markings, or null if the overall event markings are to be used.
     * @deprecated use {@link #getVisibility()}
     */
    // TODO remove-accumulo-utils
    @Deprecated(forRemoval = true)
    default Markings<?> getMarkings() {
        return AccessExpressionMarkings.createMarkings(getVisibility());
    }

    /**
     * Get the processing error for this field if any.
     *
     * @return The processing exception, or null if not set
     */
    Throwable getError();

    /**
     * Clone this object implementation
     *
     * @return the cloned object
     */
    Object clone();

    void normalize(Type<?> datawaveType);
}
