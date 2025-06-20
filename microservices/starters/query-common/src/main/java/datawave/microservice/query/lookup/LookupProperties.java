package datawave.microservice.query.lookup;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.validation.annotation.Validated;

import datawave.microservice.query.QueryParameters;
import datawave.query.data.UUIDType;

@Validated
@ConfigurationProperties(prefix = "datawave.query.lookup")
public class LookupProperties {
    @NotEmpty
    private String pool = "unassigned";
    private Map<String,UUIDType> uuidTypes = new HashMap<>();
    protected Map<String,String> contentLookupTypes = null;
    protected int batchLookupUpperLimit = 100;
    protected int tagCloudLookupUpperLimit = 500;
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

    public Map<String,UUIDType> getUuidTypes() {
        return uuidTypes;
    }

    public void setUuidTypes(Map<String,UUIDType> uuidTypes) {
        this.uuidTypes = uuidTypes;
    }

    public Map<String,String> getContentLookupTypes() {
        return contentLookupTypes;
    }

    public void setContentLookupTypes(Map<String,String> contentLookupTypes) {
        this.contentLookupTypes = contentLookupTypes;
    }

    public int getBatchLookupUpperLimit() {
        return batchLookupUpperLimit;
    }

    public void setBatchLookupUpperLimit(int batchLookupUpperLimit) {
        this.batchLookupUpperLimit = batchLookupUpperLimit;
    }

    public int getTagCloudLookupUpperLimit() {
        return tagCloudLookupUpperLimit;
    }

    public void setTagCloudLookupUpperLimit(int tagCloudLookupUpperLimit) {
        this.tagCloudLookupUpperLimit = tagCloudLookupUpperLimit;
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

    public MultiValueMap<String,String> optionalParamsToMap() {
        MultiValueMap<String,String> p = new LinkedMultiValueMap<>();
        if (this.columnVisibility != null) {
            p.put(QueryParameters.QUERY_VISIBILITY, Collections.singletonList(this.columnVisibility));
        }
        return p;
    }
}
