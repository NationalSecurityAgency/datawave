package datawave.microservice.query.lookup;

import java.util.HashMap;
import java.util.Map;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import datawave.query.data.UUIDType;

@Validated
@ConfigurationProperties(prefix = "datawave.query.lookup")
public class LookupProperties {
    @NotEmpty
    private String pool = "unassigned";
    private Map<String,UUIDType> types = new HashMap<>();
    protected int batchLookupLimit = 100;
    @NotEmpty
    protected String beginDate;
    @NotNull
    protected String columnVisibility;
    @NotEmpty
    protected String contentQueryLogicName = "ContentQuery";
    
    public String getPool() {
        return pool;
    }
    
    public void setPool(String pool) {
        this.pool = pool;
    }
    
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
    
    public String getContentQueryLogicName() {
        return contentQueryLogicName;
    }
    
    public void setContentQueryLogicName(String contentQueryLogicName) {
        this.contentQueryLogicName = contentQueryLogicName;
    }
}
