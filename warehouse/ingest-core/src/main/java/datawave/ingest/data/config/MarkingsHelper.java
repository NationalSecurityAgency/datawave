package datawave.ingest.data.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;

import datawave.ingest.data.Type;
import datawave.ingest.data.config.ingest.BaseIngestHelper;
import datawave.marking.AccessExpressionMarkings;
import datawave.marking.Markings;

public interface MarkingsHelper {

    /**
     * Parameter to specify a default marking to be used when no marking is found in the data.
     */
    String DEFAULT_MARKING = ".data.category.marking.default";

    /**
     * Parameter to specify field marking. This parameter supports multiple datatypes and fields, so a valid value would be something like
     * {@code <type>.<field>.data.field.marking}
     */
    String FIELD_MARKING = ".data.field.marking";

    /**
     * Returns the default markings for this datatype
     *
     * @return markings
     */
    Markings<?> getDefaultMarkings();

    /**
     * Returns the markings override for the field
     *
     * @param fieldName
     *            the field name
     * @return markings
     */
    Markings<?> getFieldMarking(String fieldName);

    /**
     * Method to set the markings on a field
     *
     * @param field
     *            field to mark
     */
    void markField(NormalizedContentInterface field);

    /**
     * No-op helper for default AccessExpression implementation. Should only be used for testing purposes.
     */
    class NoOp implements MarkingsHelper {

        private Map<String,Markings<?>> fieldMarkingMap = new HashMap<>();
        private Markings<?> defaultMarkings = null;

        Configuration conf;
        Type dataType;

        public NoOp(Configuration conf, Type dataType) {
            this.conf = conf;
            this.dataType = dataType;
            initDefaultMarkings();
        }

        private void initDefaultMarkings() {
            String marking = conf.get(dataType.typeName() + DEFAULT_MARKING);
            if (null != marking) {
                defaultMarkings = AccessExpressionMarkings.builder().columnVisibility(marking).build();
            }
            for (Entry<String,String> property : conf) {
                if (property.getKey().startsWith(dataType.typeName()) && property.getKey().endsWith(FIELD_MARKING)) {
                    String fieldName = null;
                    if (null != (fieldName = BaseIngestHelper.getFieldName(dataType, property.getKey(), FIELD_MARKING))) {
                        Markings<?> fieldMarking = AccessExpressionMarkings.builder().columnVisibility(property.getValue()).build();
                        fieldMarkingMap.put(fieldName, fieldMarking);
                    }
                }
            }
        }

        @Override
        public Markings<?> getDefaultMarkings() {
            return defaultMarkings;
        }

        @Override
        public Markings<?> getFieldMarking(String fieldName) {
            return fieldMarkingMap.get(fieldName);
        }

        @Override
        public void markField(NormalizedContentInterface field) {
            if (field != null) {
                if (field.getMarkings() == null) {
                    field.setMarkings(getFieldMarking(field.getIndexedFieldName()));
                }
                if (field.getMarkings() == null) {
                    field.setMarkings(defaultMarkings);
                }
            }
        }
    }
}
