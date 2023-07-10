package datawave.query.data;

public class UUIDType {

    private String fieldName = null;
    private String definedView = null;
    private Integer allowWildcardAfter = null;

    public UUIDType() {}

    public UUIDType(String field, String view, Integer allowWildcardAfter) {

        this.fieldName = field;
        this.definedView = view;
        this.allowWildcardAfter = allowWildcardAfter;
    }

    public Integer getAllowWildcardAfter() {
        return allowWildcardAfter;
    }

    public void setAllowWildcardAfter(Integer allowWildcardAfter) {
        this.allowWildcardAfter = allowWildcardAfter;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getDefinedView() {
        return definedView;
    }

    public void setDefinedView(String definedView) {
        this.definedView = definedView;
    }
}
