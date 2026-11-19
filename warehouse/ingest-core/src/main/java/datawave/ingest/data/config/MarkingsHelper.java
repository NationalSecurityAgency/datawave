package datawave.ingest.data.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;

import datawave.ingest.data.Type;
import datawave.ingest.data.config.ingest.BaseIngestHelper;

/**
 * Supplies the default event visibility and per-field visibility overrides for a datatype.
 */
public interface MarkingsHelper {

    /**
     * Parameter to specify a default visibility to be used when no visibility is found in the data.
     */
    String DEFAULT_MARKING = ".data.category.marking.default";

    /**
     * Parameter to specify a field visibility. This parameter supports multiple datatypes and fields, so a valid value would be something like
     * {@code <type>.<field>.data.field.marking}
     */
    String FIELD_MARKING = ".data.field.marking";

    /**
     * Returns the default visibility for this datatype
     *
     * @return the default visibility, or null if none is configured
     */
    ColumnVisibility getDefaultVisibility();

    /**
     * Returns the visibility override for the field
     *
     * @param fieldName
     *            the field name
     * @return the field visibility, or null if the field has no override
     */
    ColumnVisibility getFieldVisibility(String fieldName);

    /**
     * Method to set the visibility on a field that does not already have one
     *
     * @param field
     *            field to mark
     */
    void markField(NormalizedContentInterface field);

    /**
     * No-op helper for the default visibility implementation. Should only be used for testing purposes.
     */
    class NoOp implements MarkingsHelper {

        private Map<String,ColumnVisibility> fieldVisibilityMap = new HashMap<>();
        private ColumnVisibility defaultVisibility = null;

        Configuration conf;
        Type dataType;

        public NoOp(Configuration conf, Type dataType) {
            this.conf = conf;
            this.dataType = dataType;
            initVisibilities();
        }

        private void initVisibilities() {
            String marking = conf.get(dataType.typeName() + DEFAULT_MARKING);
            if (null != marking) {
                defaultVisibility = new ColumnVisibility(marking);
            }
            for (Entry<String,String> property : conf) {
                if (property.getKey().startsWith(dataType.typeName()) && property.getKey().endsWith(FIELD_MARKING)) {
                    String fieldName = null;
                    if (null != (fieldName = BaseIngestHelper.getFieldName(dataType, property.getKey(), FIELD_MARKING))) {
                        fieldVisibilityMap.put(fieldName, new ColumnVisibility(property.getValue()));
                    }
                }
            }
        }

        @Override
        public ColumnVisibility getDefaultVisibility() {
            return defaultVisibility;
        }

        @Override
        public ColumnVisibility getFieldVisibility(String fieldName) {
            return fieldVisibilityMap.get(fieldName);
        }

        @Override
        public void markField(NormalizedContentInterface field) {
            if (field != null) {
                if (FieldVisibilities.isEmpty(field.getVisibility())) {
                    field.setVisibility(getFieldVisibility(field.getIndexedFieldName()));
                }
                if (FieldVisibilities.isEmpty(field.getVisibility())) {
                    field.setVisibility(defaultVisibility);
                }
            }
        }
    }
}
