package datawave.query.data;

import java.util.Collections;
import java.util.List;

public class UUIDType {
    
    private String fieldName = null;
    private String definedView = null;
    private Integer allowWildcardAfter = null;
    private List<UUIDTransform> transforms = null;
    
    public UUIDType() {}
    
    public UUIDType(String field, String view, Integer allowWildcardAfter) {
        this(field, view, allowWildcardAfter, Collections.EMPTY_LIST);
    }
    
    public UUIDType(String field, String view, Integer allowWildcardAfter, List<UUIDTransform> transforms) {
        this.fieldName = field;
        this.definedView = view;
        this.allowWildcardAfter = allowWildcardAfter;
        this.transforms = transforms;
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
    
    public List<UUIDTransform> getTransforms() {
        return transforms;
    }
    
    public void setTransforms(List<UUIDTransform> transforms) {
        this.transforms = transforms;
    }
}
