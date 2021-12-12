package datawave.microservice.query.uuid;

import datawave.query.data.UUIDType;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.Map;

@Validated
@ConfigurationProperties(prefix = "datawave.query.uuid")
public class LookupUUIDProperties {
    private Map<String,UUIDType> types = new HashMap<>();
    protected int batchLookupLimit = 100;
    @NotEmpty
    protected String beginDate;
    @NotNull
    protected String columnVisibility;
    
    public Map<String,UUIDType> getTypes() {
        return types;
    }
    
    public void setTypes(Map<String,UUIDType> types) {
        this.types = types;
    }
    
    public int getBatchLookupLimit() {
        return batchLookupLimit;
    }
    
    public void setBatchLookupLimit(int batchLookupLimit) {
        this.batchLookupLimit = batchLookupLimit;
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
}
