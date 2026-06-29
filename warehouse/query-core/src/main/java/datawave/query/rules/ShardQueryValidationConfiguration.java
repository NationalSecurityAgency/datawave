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

    /**
     * Return a {@link MetadataHelper} instance.
     *
     * @return the metadata helper
     */
    public MetadataHelper getMetadataHelper() {
        return metadataHelper;
    }

    /**
     * Set a {@link MetadataHelper} for use by rules to validate aspects of the query that require metadata info.
     *
     * @param metadataHelper
     *            the metadata helper
     */
    public void setMetadataHelper(MetadataHelper metadataHelper) {
        this.metadataHelper = metadataHelper;
    }

    /**
     * Return a {@link TypeMetadata} instance.
     *
     * @return the metadata helper
     */
    public TypeMetadata getTypeMetadata() {
        return typeMetadata;
    }

    /**
     * Set a {@link MetadataHelper} for use by rules to validate aspects of the query that require type metadata info.
     *
     * @param typeMetadata
     *            the type metadata helper
     */
    public void setTypeMetadata(TypeMetadata typeMetadata) {
        this.typeMetadata = typeMetadata;
    }

    /**
     * Return the parsed query, typically either a {@link org.apache.commons.jexl3.parser.JexlNode} or a
     * {@link org.apache.lucene.queryparser.flexible.core.nodes.QueryNode}.
     *
     * @return the parsed query
     */
    public Object getParsedQuery() {
        return parsedQuery;
    }

    /**
     * Set the parsed query, typically either a {@link org.apache.commons.jexl3.parser.JexlNode} or a
     * {@link org.apache.lucene.queryparser.flexible.core.nodes.QueryNode}.
     *
     * @param parsedQuery
     *            the parsed query
     */
    public void setParsedQuery(Object parsedQuery) {
        this.parsedQuery = parsedQuery;
    }

    /**
     * Return the query in string form.
     *
     * @return the query string form
     */
    public String getQueryString() {
        return queryString;
    }

    /**
     * Set the string form of the query
     *
     * @param queryString
     *            the query string form
     */
    public void setQueryString(String queryString) {
        this.queryString = queryString;
    }
}
