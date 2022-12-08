package datawave.attribute;

public class EventFieldValueTuple {
    
    String fieldName;
    String value;
    
    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }
    
    public void setValue(String fieldValue) {
        value = fieldValue;
    }
    
    public String getFieldName() {
        return fieldName;
    }
    
    public String getValue() {
        return value;
    }
    
    public static String getValue(Object o) {
        if (o instanceof EventFieldValueTuple) {
            return ((EventFieldValueTuple) o).getValue();
        } else {
            return (o == null ? null : o.toString());
        }
    }
    
    public static String getFieldName(Object o) {
        if (o instanceof EventFieldValueTuple) {
            return ((EventFieldValueTuple) o).getFieldName();
        }
        return (o == null ? null : o.toString());
    }
    
}
