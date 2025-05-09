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
    private final String valueField;

    /**
     * The field values of {@link #valueField} parsed from an instance of the property {@link WhindexFieldIngestHelper#VALUES}.
     */
    private final List<String> values;

    /**
     * The source field parsed from an instance of the property {@link WhindexFieldIngestHelper#SRC_FIELD}. This is the field that may be replaced by the
     * {@link #destField}.
     */
    private final String sourceField;

    /**
     * The destination field parsed from an instance of the property {@link WhindexFieldIngestHelper#DST_FIELD}. The name of the new field (WhindexField)
     * created when a Whindex entry is generated.
     */
    private final String destField;

    /**
     * The overloaded field parsed from an instance of the property {@link WhindexFieldIngestHelper#DELETE_SRC_FIELD}. A flag indicating whether the original
     * source field should be deleted (overloaded=true) or retained (overloaded=false). The default is assumed false.
     */
    private final boolean overloaded;

    private WhindexConfig(Builder builder) {
        this.valueField = builder.valueField;
        this.values = builder.values;
        this.sourceField = builder.sourceField;
        this.destField = builder.destField;
        this.overloaded = builder.overloaded;
    }

    /**
     * Gets the field name used for checking the event map's values.
     *
     * @return the value field name
     */
    public String getValueField() {
        return valueField;
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
     * Gets the source field from the event map required for a Whindex entry.
     *
     * @return the source field name
     */
    public String getSourceField() {
        return sourceField;
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
     * Indicates whether the original source field should be deleted after processing.
     *
     * @return true if the source field is to be deleted (overloaded); false if it is to be retained
     */
    public boolean isOverloaded() {
        return overloaded;
    }

    /**
     * Compares this WhindexConfig with another object for equality. Two WhindexConfig instances are considered equal if all their properties match.
     *
     * @param o
     *            the object to compare with
     * @return true if both objects are equal; false otherwise
     */
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        WhindexConfig that = (WhindexConfig) o;
        return overloaded == that.overloaded && Objects.equals(valueField, that.valueField) && Objects.equals(values, that.values)
                        && Objects.equals(sourceField, that.sourceField) && Objects.equals(destField, that.destField);
    }

    /**
     * Computes the hash code for this WhindexConfig. The hash code is computed based on the valueField, values, sourceField, destField, and overloaded flag.
     *
     * @return the hash code as an integer
     */
    @Override
    public int hashCode() {
        return Objects.hash(valueField, values, sourceField, destField, overloaded);
    }

    /**
     * Creates a new Builder for constructing a WhindexConfig.
     *
     * @return a fresh Builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link WhindexConfig}. Use fluent setters and then call {@link #build()}.
     */
    public static class Builder {
        private String valueField;
        private List<String> values;
        private String sourceField;
        private String destField;
        private boolean overloaded = false;

        /**
         * @param valueField
         *            the field name to check against values
         */
        public Builder withValueField(String valueField) {
            this.valueField = valueField;
            return this;
        }

        /**
         * @param values
         *            the list of values that trigger a Whindex entry
         */
        public Builder withValues(List<String> values) {
            this.values = values;
            return this;
        }

        /**
         * @param sourceField
         *            the field from which to read original data
         */
        public Builder withSourceField(String sourceField) {
            this.sourceField = sourceField;
            return this;
        }

        /**
         * @param destField
         *            the field to write the Whindex entry into
         */
        public Builder withDestField(String destField) {
            this.destField = destField;
            return this;
        }

        /**
         * @param overloaded
         *            true to delete the sourceField after processing
         */
        public Builder withOverloaded(boolean overloaded) {
            this.overloaded = overloaded;
            return this;
        }

        /**
         * Builds the WhindexConfig instance. All unset fields will remain null (or false for boolean).
         *
         * @return a new immutable WhindexConfig
         */
        public WhindexConfig build() {
            return new WhindexConfig(this);
        }
    }
}
