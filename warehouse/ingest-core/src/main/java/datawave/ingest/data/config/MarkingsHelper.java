package datawave.ingest.data.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import datawave.ingest.data.Type;
import datawave.ingest.data.config.ingest.BaseIngestHelper;
import datawave.marking.MarkingFunctions;

import org.apache.hadoop.conf.Configuration;

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
    Map<String,String> getDefaultMarkings();
    
    /**
     * Returns the markings override for the field
     * 
     * @param fieldName
     *            the field name
     * @return markings
     */
    Map<String,String> getFieldMarking(String fieldName);
    
    /**
     * Method to set the markings on a field
     * 
     * @param field
     *            field to mark
     */
    void markField(NormalizedContentInterface field);
    
    /**
     * No-op helper for default ColumnVisibility implementation. Should only be used for testing purposes.
     */
    class NoOp implements MarkingsHelper {
        
        private Map<String,Map<String,String>> fieldMarkingMap = new HashMap<>();
        private Map<String,String> defaultMarkings = null;
        
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
                defaultMarkings = new HashMap<>();
                defaultMarkings.put(MarkingFunctions.Default.COLUMN_VISIBILITY, marking);
            }
            for (Entry<String,String> property : conf) {
                if (property.getKey().startsWith(dataType.typeName()) && property.getKey().endsWith(FIELD_MARKING)) {
                    String fieldName = null;
                    if (null != (fieldName = BaseIngestHelper.getFieldName(dataType, property.getKey(), FIELD_MARKING))) {
                        Map<String,String> fieldMarking = new HashMap<>();
                        fieldMarking.put(MarkingFunctions.Default.COLUMN_VISIBILITY, property.getValue());
                        fieldMarkingMap.put(fieldName, fieldMarking);
                    }
                }
            }
        }
        
        @Override
        public Map<String,String> getDefaultMarkings() {
            return defaultMarkings;
        }
        
        @Override
        public Map<String,String> getFieldMarking(String fieldName) {
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
