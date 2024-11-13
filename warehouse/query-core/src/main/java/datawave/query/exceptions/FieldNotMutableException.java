package datawave.query.exceptions;

/**
 *
 *
 */
public class FieldNotMutableException extends Exception {

    private String fieldName = "";
    private String dataType = "";

    public FieldNotMutableException(String dataType, String fieldName) {
        this.fieldName = fieldName;
        this.dataType = dataType;
    }

    @Override
    public String getMessage() {
        return super.getMessage() + " The field " + fieldName + " is not mutable for data type " + dataType
                        + ", update meta data table in order to facilitate operation.";
    }

    @Override
    public String toString() {
        return super.getMessage() + " The field " + fieldName + " is not mutable for data type " + dataType
                        + ", update meta data table in order to facilitate operation.";

    }

}
