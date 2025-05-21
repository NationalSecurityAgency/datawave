package datawave.query.tables.facets;

import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.QueryLogicTransformer;
import datawave.microservice.query.Query;
import datawave.query.Constants;
import datawave.query.DocumentSerialization;
import datawave.query.QueryParameters;
import datawave.query.attributes.Document;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.function.FacetedGrouping;
import datawave.query.function.deserializer.DocumentDeserializer;
import datawave.query.function.serializer.DocumentSerializer;
import datawave.query.iterator.QueryOptions;
import datawave.query.planner.FacetedQueryPlanner;
import datawave.query.planner.QueryPlanner;
import datawave.query.predicate.EmptyDocumentFilter;
import datawave.query.tables.IndexQueryLogic;
import datawave.query.transformer.FacetedTransformer;
import datawave.util.StringUtils;

/**
 *
 */
public class FacetedQueryLogic extends IndexQueryLogic {

    protected FacetedConfiguration facetedConfig = null;

    public FacetedQueryLogic() {
        super();
        facetedConfig = new FacetedConfiguration();
        super.setQueryPlanner(new FacetedQueryPlanner(facetedConfig));
    }

    public FacetedQueryLogic(FacetedQueryLogic other) {
        super(other);
    }

    @Override
    public FacetedQueryLogic clone() {
        return new FacetedQueryLogic(this);
    }

    public void setFacetedSearchType(FacetedSearchType type) {
        facetedConfig.setType(type);
    }

    @Override
    public QueryLogicTransformer<?,?> getTransformer(Query settings) {

        boolean reducedInSettings = false;
        String reducedResponseStr = settings.findParameter(QueryOptions.REDUCED_RESPONSE).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(reducedResponseStr)) {
            reducedInSettings = Boolean.parseBoolean(reducedResponseStr);
        }
        boolean reduced = (this.isReducedResponse() || reducedInSettings);
        FacetedTransformer transformer = new FacetedTransformer(this, settings, markingFunctions, responseObjectFactory, reduced);
        transformer.setEventQueryDataDecoratorTransformer(eventQueryDataDecoratorTransformer);

        transformer.setQm(queryModel);

        return transformer;
    }

    @Override
    public GenericQueryConfiguration initialize(AccumuloClient client, Query settings, Set<Authorizations> auths) throws Exception {

        facetedConfig = ((FacetedQueryPlanner) getQueryPlanner()).getConfiguration();

        // Get the list of fields to project up the stack. May be null.
        final String facetedFields = settings.findParameter(FacetedConfiguration.FACETED_FIELDS).getParameterValue().trim();

        if (org.apache.commons.lang.StringUtils.isNotBlank(facetedFields)) {
            Set<String> facetedFieldSet = Sets.newHashSet(StringUtils.split(facetedFields, Constants.PARAM_VALUE_SEP));

            // Only set the projection fields if we were actually given some
            if (!facetedFieldSet.isEmpty()) {
                facetedConfig.setFacetedFields(facetedFieldSet);
            }
        }

        final String limitFieldsString = settings.findParameter(QueryParameters.LIMIT_FIELDS).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(limitFieldsString)) {
            boolean limitFields = Boolean.parseBoolean(limitFieldsString);
            facetedConfig.setHasFieldLimits(limitFields);
        }

        final String streamingEnabledStr = settings.findParameter(FacetedConfiguration.STREAMING_ENABLED).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(streamingEnabledStr)) {
            boolean streamingEnabled = Boolean.parseBoolean(streamingEnabledStr);
            facetedConfig.setStreamingMode(streamingEnabled);
        }

        final String facetedType = settings.findParameter(FacetedConfiguration.FACETED_SEARCH_TYPE).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(facetedType)) {
            facetedConfig.setType(FacetedSearchType.valueOf(facetedType));
        }

        final String maximumGroupCount = settings.findParameter(FacetedConfiguration.MAXIMUM_GROUP_COUNT).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(maximumGroupCount)) {
            facetedConfig.setMaximumFacetGroupCount(Integer.parseInt(maximumGroupCount));
        }

        final String minimumCount = settings.findParameter(FacetedConfiguration.MINIMUM_COUNT).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(minimumCount)) {
            facetedConfig.setMinimumCount(Integer.parseInt(minimumCount));
        }

        return super.initialize(client, settings, auths);
    }

    @Override
    public void setupQuery(GenericQueryConfiguration configuration) throws Exception {

        super.setupQuery(configuration);

        /*
         * A few required components for document serialization and deserialization to be used later
         */
        final Query myQuery = ((ShardQueryConfiguration) configuration).getQuery();

        final DocumentDeserializer deserializer = DocumentSerialization.getDocumentDeserializer(myQuery);

        final DocumentSerializer serializer = DocumentSerialization.getDocumentSerializer(myQuery);

        List<Function<Entry<Key,Document>,Entry<Key,Document>>> functionList = Lists.newArrayList();

        functionList.add(new FacetedGrouping(facetedConfig));

        EmptyValueFunction filter = new EmptyValueFunction(deserializer);

        List<Predicate<Entry<Key,Value>>> filterList = Lists.newArrayList();

        filterList.add(filter);

        iterator = new MergedReadAhead<>(facetedConfig.isStreaming, iterator, new FacetedFunction(deserializer, serializer, functionList), filterList);

    }

    protected static class EmptyValueFunction implements Predicate<Entry<Key,Value>> {
        private final EmptyDocumentFilter filter;
        private final DocumentDeserializer deserializer;

        public EmptyValueFunction(DocumentDeserializer deserializer) {
            filter = new EmptyDocumentFilter();
            this.deserializer = deserializer;
        }

        @Override
        public boolean apply(Entry<Key,Value> input) {

            return filter.apply(deserializer.apply(input));
        }

    }

    @Override
    public Set<String> getOptionalQueryParameters() {
        Set<String> optionalParams = new TreeSet<>(super.getOptionalQueryParameters());
        optionalParams.add(FacetedConfiguration.MINIMUM_COUNT);
        optionalParams.add(FacetedConfiguration.FACETED_SEARCH_TYPE);
        optionalParams.add(FacetedConfiguration.FACETED_FIELDS);
        optionalParams.add(FacetedConfiguration.MAXIMUM_GROUP_COUNT);
        optionalParams.add(FacetedConfiguration.STREAMING_ENABLED);
        return optionalParams;
    }

    @Override
    public void setFullTableScanEnabled(boolean fullTableScanEnabled) {
        Preconditions.checkArgument(!fullTableScanEnabled, "The FacetedQueryLogic does not support full-table scans");
        super.setFullTableScanEnabled(false);
    }

    /**
     * @param i
     *            minimum facet count
     */
    public void setMinimumFacet(final int i) {
        facetedConfig.setMinimumCount(i);
    }

    public void setMaximumFacetGrouping(final int maxGroup) {
        facetedConfig.setMaximumFacetGroupCount(maxGroup);
    }

    public void setFacetTableName(String facetTableName) {
        facetedConfig.setFacetTableName(facetTableName);
    }

    public void setFacetMetadataTableName(String facetMetadataTableName) {
        facetedConfig.setFacetMetadataTableName(facetMetadataTableName);
    }

    public void setFacetHashTableName(String facetHashTableName) {
        facetedConfig.setFacetHashTableName(facetHashTableName);
    }

    public void setQueryPlanner(QueryPlanner planner) {
        log.debug("Intercepting call to setQueryPlanner() an translating to a no-op to retain FacetedQueryPlanner");
    }

    protected FacetedQueryPlanner getPlanner() {
        return (FacetedQueryPlanner) this.getQueryPlanner();
    }

    /**
     * @param facet
     *            a field name for faceting
     */
    public void addFacet(String facet) {
        facetedConfig.addFacetedField(facet);
    }

    /**
     * @param isStreaming
     *            whether the query will stream results
     */
    public void setStreaming(boolean isStreaming) {
        facetedConfig.setStreamingMode(isStreaming);
    }

}
