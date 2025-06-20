package datawave.microservice.query.translateid;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.validation.annotation.Validated;

import datawave.microservice.query.QueryParameters;
import datawave.query.data.UUIDType;

@Validated
@ConfigurationProperties(prefix = "datawave.query.translateid")
public class TranslateIdProperties {
    private Map<String,UUIDType> uuidTypes;
    @NotEmpty
    private String beginDate;
    @NotNull
    private String columnVisibility;
    private Set<String> allowedQueryParameters = new HashSet<>(Arrays.asList(QueryParameters.QUERY_PAGESIZE, QueryParameters.QUERY_PAGETIMEOUT));
    @NotEmpty
    private String queryLogicName = "IdTranslationQuery";
    @NotEmpty
    private String tldQueryLogicName = "IdTranslationTLDQuery";
    private String allowedUUIDQueryLogicName = "LuceneUUIDEventQuery";

    public Map<String,UUIDType> getUuidTypes() {
        return uuidTypes;
    }

    public void setUuidTypes(Map<String,UUIDType> uuidTypes) {
        Map<String,UUIDType> allowedTypes = new HashMap<>();
        if (allowedUUIDQueryLogicName != null && uuidTypes != null) {
            for (Map.Entry<String,UUIDType> uuidType : uuidTypes.entrySet()) {
                if (allowedUUIDQueryLogicName.equalsIgnoreCase(uuidType.getValue().getQueryLogic("default"))) {
                    allowedTypes.put(uuidType.getKey(), uuidType.getValue());
                }
            }
        }
        this.uuidTypes = allowedTypes;
    }

    public String getBeginDate() {
        return beginDate;
    }

    public void setBeginDate(String beginDate) {
        this.beginDate = beginDate;
    }

    public String getColumnVisibility() {
        return columnVisibility;
    }

    public void setColumnVisibility(String columnVisibility) {
        this.columnVisibility = columnVisibility;
    }

    public Set<String> getAllowedQueryParameters() {
        return allowedQueryParameters;
    }

    public void setAllowedQueryParameters(Set<String> allowedQueryParameters) {
        this.allowedQueryParameters = allowedQueryParameters;
    }

    public String getQueryLogicName() {
        return queryLogicName;
    }

    public void setQueryLogicName(String queryLogicName) {
        this.queryLogicName = queryLogicName;
    }

    public String getTldQueryLogicName() {
        return tldQueryLogicName;
    }

    public void setTldQueryLogicName(String tldQueryLogicName) {
        this.tldQueryLogicName = tldQueryLogicName;
    }

    public String getAllowedUUIDQueryLogicName() {
        return allowedUUIDQueryLogicName;
    }

    public void setAllowedUUIDQueryLogicName(String allowedUUIDQueryLogicName) {
        this.allowedUUIDQueryLogicName = allowedUUIDQueryLogicName;

        if (this.allowedUUIDQueryLogicName != null && uuidTypes != null) {
            List<String> entriesToRemove = new ArrayList<>();
            for (Map.Entry<String,UUIDType> uuidEntry : uuidTypes.entrySet()) {
                if (allowedUUIDQueryLogicName.equalsIgnoreCase(uuidEntry.getValue().getQueryLogic("default"))) {
                    entriesToRemove.add(uuidEntry.getKey());
                }
            }
            for (String uuidKey : entriesToRemove) {
                uuidTypes.remove(uuidKey);
            }
        }
    }

    public MultiValueMap<String,String> optionalParamsToMap() {
        MultiValueMap<String,String> p = new LinkedMultiValueMap<>();
        if (this.columnVisibility != null) {
            p.put(QueryParameters.QUERY_VISIBILITY, Collections.singletonList(this.columnVisibility));
        }
        return p;
    }
}
