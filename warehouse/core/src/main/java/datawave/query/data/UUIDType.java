package datawave.query.data;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

public class UUIDType {

    public static final String DEFAULT_VIEW = "default";

    private String fieldName = null;
    private Integer allowWildcardAfter = null;

    private final Map<String,String> definedViews = new HashMap<>();

    public UUIDType() {}

    public UUIDType(String field, String view, Integer allowWildcardAfter) {

        this.fieldName = field;
        this.allowWildcardAfter = allowWildcardAfter;

        this.definedViews.put(DEFAULT_VIEW, view);
    }

    public UUIDType(String field, Map<String,String> views, Integer allowWildcardAfter) {
        this.fieldName = field;
        this.allowWildcardAfter = allowWildcardAfter;

        this.definedViews.putAll(views);
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

    public String getDefinedView(String context) {
        if (StringUtils.isEmpty(context)) {
            context = DEFAULT_VIEW;
        }
        return getDefinedViews().get(context);
    }

    public Map<String,String> getDefinedViews() {
        return definedViews;
    }

    public void setDefinedViews(Map<String,String> views) {
        this.definedViews.clear();
        this.definedViews.putAll(views);
    }

    public void setDefinedView(String context, String view) {
        if (StringUtils.isEmpty(context)) {
            context = DEFAULT_VIEW;
        }
        getDefinedViews().put(context, view);
    }
}
