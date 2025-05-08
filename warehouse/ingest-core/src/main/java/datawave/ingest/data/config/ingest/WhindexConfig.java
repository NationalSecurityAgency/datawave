package datawave.ingest.data.config.ingest;

import java.util.List;
import java.util.Objects;

/**
 * Container class for keeping track of configuration rules related to Whindex entries.
 */
public class WhindexConfig {

    /**
     * The field name parsed from an instance of the property {@link WhindexFieldIngestHelper#VALUE_FIELD}.
     */
    private String valueField;

    /**
     * The field values of {@link #valueField} parsed from an instance of the property {@link WhindexFieldIngestHelper#VALUES}.
     */
    private List<String> values;

    /**
     * The source field parsed from an instance of the property {@link WhindexFieldIngestHelper#SRC_FIELD}.
     * This is the field that may be replaced by the {@link #destField}.
     */
    private String sourceField;

    /**
     * The destination field parsed from an instance of the property {@link WhindexFieldIngestHelper#DST_FIELD}.
     * The name of the new field (WhindexField) created when a Whindex entry is generated.
     */
    private String destField;

    /**
     * The overloaded field parsed from an instance of the property {@link WhindexFieldIngestHelper#DELETE_SRC_FIELD}.
     *  A flag indicating whether the original source field should be deleted (overloaded=true) or retained (overloaded=false).
     *  The default is assumed false.
     */
    private boolean overloaded;

    /**
     * Gets the field name used for checking the event map's values.
     *
     * @return the value field name
     */
    public String getValueField() {
        return valueField;
    }

    /**
     * Sets the field name to be used for checking the event map's values.
     *
     * @param valueField
     *            the value field name to set
     */
    public void setValueField(String valueField) {
        this.valueField = valueField;
    }

    /**
     * Gets the list of values that need to be present in the event map's valueField for a Whindex entry.
     *
     * @return the list of values
     */
    public List<String> getValues() {
        return values;
    }

    /**
     * Sets the list of values that trigger the creation of a Whindex entry.
     *
     * @param values
     *            the list of values to set
     */
    public void setValues(List<String> values) {
        this.values = values;
    }

    /**
     * Gets the source field from the event map required for a Whindex entry.
     *
     * @return the source field name
     */
    public String getSourceField() {
        return sourceField;
    }

    /**
     * Sets the source field from the event map that is required for creating a Whindex entry.
     *
     * @param sourceField
     *            the source field name to set
     */
    public void setSourceField(String sourceField) {
        this.sourceField = sourceField;
    }

    /**
     * Gets the destination field (WhindexField) created when a Whindex entry is generated.
     *
     * @return the destination field name
     */
    public String getDestField() {
        return destField;
    }

    /**
     * Sets the destination field (WhindexField) name.
     *
     * @param destField
     *            the destination field name to set
     */
    public void setDestField(String destField) {
        this.destField = destField;
    }

    /**
     * Indicates whether the original source field should be deleted after processing.
     *
     * @return true if the source field is to be deleted (overloaded); false if it is to be retained
     */
    public boolean isOverloaded() {
        return overloaded;
    }

    /**
     * Sets the overloaded flag, which determines whether the original source field should be deleted once a Whindex entry has been processed.
     *
     * @param overloaded
     *            true to delete the source field; false to keep it
     */
    public void setOverloaded(boolean overloaded) {
        this.overloaded = overloaded;
    }

    /**
     * Compares this WhindexConfig with another object for equality.
     * <p>
     * Two WhindexConfig instances are considered equal if all their properties match.
     * </p>
     *
     * @param o
     *            the object to compare with
     * @return true if both objects are equal; false otherwise
     */
    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass())
            return false;
        WhindexConfig config = (WhindexConfig) o;
        return overloaded == config.overloaded && Objects.equals(valueField, config.valueField) && Objects.equals(values, config.values)
                        && Objects.equals(sourceField, config.sourceField) && Objects.equals(destField, config.destField);
    }

    /**
     * Computes the hash code for this WhindexConfig.
     * <p>
     * The hash code is computed based on the valueField, values, sourceField, destField, and overloaded flag.
     * </p>
     *
     * @return the hash code as an integer
     */
    @Override
    public int hashCode() {
        return Objects.hash(valueField, values, sourceField, destField, overloaded);
    }
}
