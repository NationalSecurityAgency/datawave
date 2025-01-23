package datawave.query.rules;

import datawave.query.util.MetadataHelper;
import datawave.query.util.TypeMetadata;

/**
 * Base implementation of {@link QueryValidationConfiguration} that is specific to shard queries.
 */
public class ShardQueryValidationConfiguration extends QueryValidationConfigurationImpl {

    protected MetadataHelper metadataHelper;
    protected TypeMetadata typeMetadata;
    protected Object parsedQuery;
    protected String queryString;

    public MetadataHelper getMetadataHelper() {
        return metadataHelper;
    }

    public void setMetadataHelper(MetadataHelper metadataHelper) {
        this.metadataHelper = metadataHelper;
    }

    public TypeMetadata getTypeMetadata() {
        return typeMetadata;
    }

    public void setTypeMetadata(TypeMetadata typeMetadata) {
        this.typeMetadata = typeMetadata;
    }

    public Object getParsedQuery() {
        return parsedQuery;
    }

    public void setParsedQuery(Object parsedQuery) {
        this.parsedQuery = parsedQuery;
    }

    public String getQueryString() {
        return queryString;
    }

    public void setQueryString(String queryString) {
        this.queryString = queryString;
    }
}
