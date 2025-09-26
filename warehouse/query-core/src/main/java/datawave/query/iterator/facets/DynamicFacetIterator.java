package datawave.query.iterator.facets;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;

import javax.annotation.Nullable;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.log4j.Logger;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;

import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import datawave.query.attributes.Document;
import datawave.query.function.Aggregation;
import datawave.query.function.AttributeToCardinality;
import datawave.query.function.CardinalitySummation;
import datawave.query.function.DocumentCountCardinality;
import datawave.query.function.JexlEvaluation;
import datawave.query.function.KeyToDocumentData;
import datawave.query.function.MinimumEstimation;
import datawave.query.iterator.AccumuloTreeIterable;
import datawave.query.iterator.FieldIndexOnlyQueryIterator;
import datawave.query.iterator.aggregation.DocumentData;
import datawave.query.iterator.builder.CardinalityIteratorBuilder;
import datawave.query.jexl.functions.CardinalityAggregator;
import datawave.query.jexl.functions.FieldIndexAggregator;
import datawave.query.jexl.visitors.IteratorBuildingVisitor;
import datawave.query.predicate.EventDataQueryFieldFilter;
import datawave.query.tables.facets.FacetedConfiguration;
import datawave.query.tables.facets.FacetedSearchType;
import datawave.query.util.TypeMetadata;

/**
 *
 */
public class DynamicFacetIterator extends FieldIndexOnlyQueryIterator {
    private static final Logger log = Logger.getLogger(DynamicFacetIterator.class);

    public static final String FACETED_SEARCH_TYPE = "query.facet.type";
    public static final String FACETED_MINIMUM = "query.facet.minimum";
    public static final String FACETED_SEARCH_FIELDS = "query.facet.fields";

    FacetedConfiguration configuration;

    Map<String,String> documenIteratorOptions;

    protected boolean merge = false;

    @Override
    public IteratorOptions describeOptions() {
        IteratorOptions parentOpts = super.describeOptions();

        Map<String,String> options = parentOpts.getNamedOptions();

        options.put(FACETED_SEARCH_TYPE, "Type of faceted search");
        options.put(FACETED_MINIMUM, "Minimum Facet count. Defaults to 0");
        options.put(FACETED_SEARCH_FIELDS, "Comma separated list of facets that we must include. If this is empty, we return all facets");

        return new IteratorOptions(getClass().getSimpleName(), "Runs a Faceted search against event data", options, null);
    }

    @Override
    public boolean validateOptions(Map<String,String> options) {
        boolean res = super.validateOptions(options);

        configuration = new FacetedConfiguration();

        FacetedSearchType type = FacetedSearchType.DAY_COUNT;
        if (options.containsKey(FACETED_SEARCH_TYPE)) {
            type = FacetedSearchType.valueOf(options.get(FACETED_SEARCH_TYPE));
        }

        configuration.setType(type);

        if (options.containsKey(FACETED_MINIMUM)) {
            try {
                configuration.setMinimumCount(Integer.parseInt(options.get(FACETED_MINIMUM)));
            } catch (NumberFormatException nfe) {
                log.error(nfe);
                // defaulting to 1
            }
        }

        String fields = "";
        if (options.containsKey(FACETED_SEARCH_FIELDS)) {

            fields = options.get(FACETED_SEARCH_FIELDS);

        }

        switch (type) {
            case SHARD_COUNT:
            case DAY_COUNT:
                // Parse & flatten the query tree.
                getScript();
                myEvaluationFunction = new JexlEvaluation(this.getQuery(), arithmetic);
                break;
            default:
                break;
        }

        SortedSet<String> facetedFields = Sets.newTreeSet(Splitter.on(",").split(fields));

        // add faceted fields to the index only field list, then remove the key list of
        // indexed fields from the faceted field list.
        if (!facetedFields.isEmpty())
            configuration.setHasFieldLimits(true);

        configuration.setFacetedFields(facetedFields);

        // assign the options for later use by the document iterator
        documenIteratorOptions = options;

        return res;
    }

    @Override
    protected IteratorBuildingVisitor createIteratorBuildingVisitor(final Range documentRange, boolean isQueryFullySatisfied, boolean sortedUIDs)
                    throws MalformedURLException, ConfigException, IllegalAccessException, InstantiationException {
        //  @formatter:off
        return super.createIteratorBuildingVisitor(documentRange, isQueryFullySatisfied, sortedUIDs)
                .setIteratorBuilder(CardinalityIteratorBuilder.class)
                .setFieldsToAggregate(configuration.getFacetedFields())
                //  note: setting query fully satisfied to false kicks the document building decision
                //  to the set of fields to aggregate, which is the set of facet fields configured
                //  for this query. The FacetLogic should not rely on arbitrary decisions from
                //  the SatisfactionVisitor.
                .setIsQueryFullySatisfied(false);
        //  @formatter:on
    }

    @Override
    public Iterator<Entry<Key,Document>> getDocumentIterator(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
                    throws IOException, ConfigException, InstantiationException, IllegalAccessException {
        // Otherwise, we have to use the field index
        // Seek() the boolean logic stuff
        createAndSeekIndexIterator(range, columnFamilies, inclusive);

        Function<Entry<Key,Document>,Entry<DocumentData,Document>> keyToDoc = null;

        // TODO consider using the new EventDataQueryExpressionFilter
        EventDataQueryFieldFilter projection = null;

        Iterator<Entry<Key,Document>> documents = null;

        if (!configuration.getFacetedFields().isEmpty()) {
            projection = new EventDataQueryFieldFilter().withFields(configuration.getFacetedFields());
        }

        if (!configuration.hasFieldLimits() || projection != null) {
            keyToDoc = new KeyToDocumentData(source.deepCopy(myEnvironment), getEquality(), projection, this.includeHierarchyFields,
                            this.includeHierarchyFields).withRangeProvider(getRangeProvider()).withAggregationThreshold(getDocAggregationThresholdMs());
        }

        AccumuloTreeIterable<Key,DocumentData> doc = null;
        if (null != keyToDoc) {
            doc = new AccumuloTreeIterable<>(fieldIndexResults.tree, keyToDoc);
        } else {
            if (log.isTraceEnabled()) {
                log.trace("Skipping document lookup, because we don't need it");
            }
            doc = new AccumuloTreeIterable<>(fieldIndexResults.tree, new Function<Entry<Key,Document>,Entry<DocumentData,Document>>() {

                @Override
                @Nullable
                public Entry<DocumentData,Document> apply(@Nullable Entry<Key,Document> input) {

                    Set<Key> docKeys = Sets.newHashSet();

                    List<Entry<Key,Value>> attrs = Lists.newArrayList();

                    return Maps.immutableEntry(new DocumentData(input.getKey(), docKeys, attrs, true), input.getValue());
                }

            });
        }

        doc.seek(range, columnFamilies, inclusive);

        TypeMetadata typeMetadata = this.getTypeMetadata();

        documents = Iterators.transform(doc.iterator(), new Aggregation(this.getTimeFilter(), typeMetadata, compositeMetadata, this.isIncludeGroupingContext(),
                        this.includeRecordId, false, null));

        switch (configuration.getType()) {
            case SHARD_COUNT:
            case DAY_COUNT:

                SortedKeyValueIterator<Key,Value> sourceDeepCopy = source.deepCopy(myEnvironment);

                documents = getEvaluation(sourceDeepCopy, documents, compositeMetadata, typeMetadata, columnFamilies, inclusive);

                // Take the document Keys and transform it into Entry<Key,Document>, removing Attributes for this Document
                // which do not fall within the expected time range
                documents = Iterators.transform(documents, new DocumentCountCardinality(configuration.getType(), !merge));
            default:
                break;
        }

        return documents;

    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("Seek range: " + range);
        }

        this.range = range;

        Iterator<Entry<Key,Document>> fieldIndexDocuments = null;
        try {
            fieldIndexDocuments = getDocumentIterator(range, columnFamilies, inclusive);
        } catch (ConfigException | IllegalAccessException | InstantiationException e) {
            throw new IOException("Unable to create document iterator", e);
        }

        // at this point we should have the cardinality for all fields
        // so we should convert each Attribute into a Cardinality
        fieldIndexDocuments = Iterators.transform(fieldIndexDocuments, new AttributeToCardinality());

        // convert the stream into a single document, so that we can summarize the cardinality
        fieldIndexDocuments = summarize(fieldIndexDocuments);

        // minimize the list of facets that are returned.
        fieldIndexDocuments = Iterators.transform(fieldIndexDocuments, new MinimumEstimation(configuration.getMinimumFacetCount()));

        documentIterator = fieldIndexDocuments;

        prepareKeyValue();
    }

    protected Iterator<Entry<Key,Document>> summarize(Iterator<Entry<Key,Document>> fieldIndexDocuments) {

        if (fieldIndexDocuments.hasNext()) {
            Entry<Key,Document> topEntry = fieldIndexDocuments.next();

            CardinalitySummation summarizer = new CardinalitySummation(topEntry.getKey(), topEntry.getValue(), merge);
            while (fieldIndexDocuments.hasNext()) {
                topEntry = fieldIndexDocuments.next();
                summarizer.apply(topEntry);
            }

            return Iterators.singletonIterator(summarizer.getTop());
        } else
            return fieldIndexDocuments;
    }

    /**
     * Get a FieldIndexAggregator
     *
     * @return a {@link CardinalityAggregator}
     */
    @Override
    public FieldIndexAggregator getFiAggregator() {
        if (fiAggregator == null) {
            fiAggregator = new CardinalityAggregator(getAllIndexOnlyFields(), !merge);
        }
        return fiAggregator;
    }

}
