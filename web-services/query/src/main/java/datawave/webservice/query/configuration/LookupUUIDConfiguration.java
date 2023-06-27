package datawave.webservice.query.configuration;

import java.util.List;

import javax.ws.rs.core.MultivaluedMap;

import org.jboss.resteasy.specimpl.MultivaluedMapImpl;

import datawave.microservice.query.QueryParameters;
import datawave.query.data.UUIDType;
import datawave.webservice.query.util.LookupUUIDConstants;

/**
 * Configuration bean for lookupUUID web service endpoints
 */
public class LookupUUIDConfiguration {

    protected List<UUIDType> uuidTypes = null;
    protected int batchLookupUpperLimit = LookupUUIDConstants.DEFAULT_BATCH_LOOKUP_UPPER_LIMIT;
    protected String beginDate = null;
    protected String columnVisibility;

    /**
     * Returns the maximum number of UUIDs allowed for batch lookup. A zero or negative value is interpreted as unlimited. The default value is 100.
     *
     * @return the maximum number of UUIDs allowed for batch lookup
     */
    public int getBatchLookupUpperLimit() {
        return this.batchLookupUpperLimit;
    }

    public String getBeginDate() {
        return this.beginDate;
    }

    public String getColumnVisibility() {
        return this.columnVisibility;
    }

    public List<UUIDType> getUuidTypes() {
        return this.uuidTypes;
    }

    /**
     * Sets the maximum number of UUIDs allowed for batch lookup. A zero or negative value is interpreted as unlimited.
     *
     * @param batchLookupUpperLimit
     *            the maximum number of UUIDs allowed for batch lookup
     */
    public void setBatchLookupUpperLimit(int batchLookupUpperLimit) {
        this.batchLookupUpperLimit = batchLookupUpperLimit;
    }

    public void setBeginDate(String beginDate) {
        this.beginDate = beginDate;
    }

    public void setColumnVisibility(String columnVisibility) {
        this.columnVisibility = columnVisibility;
    }

    public void setUuidTypes(List<UUIDType> uuidTypes) {
        this.uuidTypes = uuidTypes;
    }

    public MultivaluedMap<String,String> optionalParamsToMap() {
        MultivaluedMap<String,String> p = new MultivaluedMapImpl<>();
        if (this.columnVisibility != null) {
            p.putSingle(QueryParameters.QUERY_VISIBILITY, this.columnVisibility);
        }
        return p;
    }
}
