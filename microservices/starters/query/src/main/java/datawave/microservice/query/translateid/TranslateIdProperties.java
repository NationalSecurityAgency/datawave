package datawave.microservice.query.translateid;

import static datawave.microservice.query.QueryParameters.QUERY_PAGESIZE;
import static datawave.microservice.query.QueryParameters.QUERY_PAGETIMEOUT;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import datawave.query.data.UUIDType;

@Validated
@ConfigurationProperties(prefix = "datawave.query.translateid")
public class TranslateIdProperties {
    private Map<String,UUIDType> types;
    @NotEmpty
    private String beginDate;
    @NotNull
    private String columnVisibility;
    private Set<String> allowedQueryParameters = new HashSet<>(Arrays.asList(QUERY_PAGESIZE, QUERY_PAGETIMEOUT));
    @NotEmpty
    private String queryLogicName = "IdTranslationQuery";
    @NotEmpty
    private String tldQueryLogicName = "IdTranslationTLDQuery";
    private String allowedUUIDQueryLogicName = "LuceneUUIDEventQuery";
    
    public Map<String,UUIDType> getTypes() {
        return types;
    }
    
    public void setTypes(Map<String,UUIDType> types) {
        Map<String,UUIDType> allowedTypes = new HashMap<>();
        if (allowedUUIDQueryLogicName != null && types != null) {
            for (Map.Entry<String,UUIDType> uuidType : types.entrySet()) {
                if (allowedUUIDQueryLogicName.equalsIgnoreCase(uuidType.getValue().getQueryLogic("default"))) {
                    allowedTypes.put(uuidType.getKey(), uuidType.getValue());
                }
            }
        }
        this.types = allowedTypes;
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
        
        if (this.allowedUUIDQueryLogicName != null && types != null) {
            List<String> entriesToRemove = new ArrayList<>();
            for (Map.Entry<String,UUIDType> uuidEntry : types.entrySet()) {
                if (allowedUUIDQueryLogicName.equalsIgnoreCase(uuidEntry.getValue().getQueryLogic("default"))) {
                    entriesToRemove.add(uuidEntry.getKey());
                }
            }
            for (String uuidKey : entriesToRemove) {
                types.remove(uuidKey);
            }
        }
    }
}
