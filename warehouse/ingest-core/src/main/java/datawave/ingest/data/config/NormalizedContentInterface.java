package datawave.ingest.data.config;

import java.util.Map;

import datawave.data.type.Type;

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
     * Set the field specific markings
     *
     * @param markings
     *            the markings to set
     */
    void setMarkings(Map<String,String> markings);

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
     */
    Map<String,String> getMarkings();

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
