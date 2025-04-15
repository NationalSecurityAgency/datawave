package datawave.ingest.data.config.ingest;

import java.util.List;
import java.util.Objects;

/**
 * Container class for keeping track of configuration rules related to Whindex entries.
 * <p>
 * A whindex rule configuration is defined by several parameters:
 * <ul>
 *   <li><strong>valueField</strong> - The field in which an event map's values are checked.
 *       The event map must contain a matching entry for this field in order to create a Whindex entry.</li>
 *   <li><strong>values</strong> - A list of values that need to be present in the event map's
 *      valueField to create a whindex entry.</li>
 *   <li><strong>sourceField</strong> - A field that must be present in the event map for a Whindex entry to be created.
 *       Depending on configuration, this field may be replaced with the destination field.</li>
 *   <li><strong>destField</strong> - Also known as the "WhindexField". This is the name of the new field
 *       created when a whindex entry is generated.</li>
 *   <li><strong>overloaded</strong> - A boolean flag indicating whether the original source field should
 *       be deleted (true) or retained (false) once the whindex entry has been processed. Inclusion of this is optional,
 *       the default value is assumed false.</li>
 * </ul>
 * </p>
 */
public class WhindexConfig {

    // The field in which the event map's values are checked.
    private String valueField;

    // The list of values that must be present in the event map's valueField to trigger a Whindex entry.
    private List<String> values;

    // The field from the event map that is required for creating a Whindex entry.
    // This field might be replaced by the destination field based on the configuration.
    private String sourceField;

    // The name of the new field (WhindexField) created when a Whindex entry is generated.
    private String destField;

    // A flag indicating whether the original source field should be deleted (overloaded=true)
    // or retained (overloaded=false). The default is assumed false.
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
     * @param valueField the value field name to set
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
     * @param values the list of values to set
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
     * @param sourceField the source field name to set
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
     * @param destField the destination field name to set
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
     * Sets the overloaded flag, which determines whether the original source field should be deleted
     * once a Whindex entry has been processed.
     *
     * @param overloaded true to delete the source field; false to keep it
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
     * @param o the object to compare with
     * @return true if both objects are equal; false otherwise
     */
    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass())
            return false;
        WhindexConfig config = (WhindexConfig) o;
        return overloaded == config.overloaded
                && Objects.equals(valueField, config.valueField)
                && Objects.equals(values, config.values)
                && Objects.equals(sourceField, config.sourceField)
                && Objects.equals(destField, config.destField);
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
