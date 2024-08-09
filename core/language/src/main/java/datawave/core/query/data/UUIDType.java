package datawave.core.query.data;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

public class UUIDType {

    public static final String DEFAULT_LOGIC = "default";

    private String fieldName = null;
    private Integer allowWildcardAfter = null;

    private final Map<String,String> queryLogics = new HashMap<>();

    public UUIDType() {}

    public UUIDType(String field, String queryLogic, Integer allowWildcardAfter) {

        this.fieldName = field;
        this.allowWildcardAfter = allowWildcardAfter;

        this.queryLogics.put(DEFAULT_LOGIC, queryLogic);
    }

    public UUIDType(String field, Map<String,String> queryLogics, Integer allowWildcardAfter) {
        this.fieldName = field;
        this.allowWildcardAfter = allowWildcardAfter;

        this.queryLogics.putAll(queryLogics);
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

    public String getQueryLogic(String context) {
        if (StringUtils.isEmpty(context)) {
            context = DEFAULT_LOGIC;
        }
        return getQueryLogics().get(context);
    }

    public Map<String,String> getQueryLogics() {
        return queryLogics;
    }

    public void setQueryLogics(Map<String,String> queryLogics) {
        this.queryLogics.clear();
        this.queryLogics.putAll(queryLogics);
    }

    public void setQueryLogic(String context, String queryLogic) {
        if (StringUtils.isEmpty(context)) {
            context = DEFAULT_LOGIC;
        }
        getQueryLogics().put(context, queryLogic);
    }
}
