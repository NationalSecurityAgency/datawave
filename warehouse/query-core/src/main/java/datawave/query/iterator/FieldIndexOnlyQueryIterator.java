package datawave.query.iterator;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import datawave.query.Constants;
import datawave.query.DocumentSerialization.ReturnType;
import datawave.query.attributes.Document;
import datawave.query.function.DataTypeAsField;
import datawave.query.function.GetStartKey;
import datawave.query.function.LogTiming;
import datawave.query.iterator.errors.UnindexedException;
import datawave.query.iterator.filter.EventKeyDataTypeFilter;
import datawave.query.iterator.filter.FieldIndexKeyDataTypeFilter;
import datawave.query.iterator.filter.KeyIdentity;
import datawave.query.iterator.filter.StringToText;
import datawave.query.iterator.profile.EvaluationTrackingFunction;
import datawave.query.iterator.profile.QuerySpan;
import datawave.query.iterator.profile.SourceTrackingIterator;
import datawave.query.jexl.functions.FieldIndexAggregator;
import datawave.query.jexl.functions.IdentityAggregator;
import datawave.query.jexl.visitors.IteratorBuildingVisitor;
import datawave.query.jexl.visitors.SatisfactionVisitor;
import datawave.query.predicate.TimeFilter;
import datawave.util.StringUtils;

/**
 *
 */
public class FieldIndexOnlyQueryIterator extends QueryIterator {
    private static final Logger log = Logger.getLogger(FieldIndexOnlyQueryIterator.class);

    protected AccumuloFieldIndexIterable fieldIndexResults;

    public FieldIndexOnlyQueryIterator() {
        super();
    }

    public FieldIndexOnlyQueryIterator(FieldIndexOnlyQueryIterator other, IteratorEnvironment env) {
        super(other, env);
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        return new FieldIndexOnlyQueryIterator(this, env);
    }

    @Override
    public IteratorOptions describeOptions() {
        Map<String,String> options = new HashMap<>();

        options.put(QUERY, "The JEXL query to evaluate documents against");
        options.put(QUERY_ID, "The query id");
        options.put(TYPE_METADATA, "encapsulation of a map of field name to a multimap of ingest-type to DataType class names");
        options.put(TYPE_METADATA_AUTHS, "subset of metadata auths that the user has. Used as a key for the TypeMetadataProvider");
        options.put(QUERY_MAPPING_COMPRESS, "Boolean value to indicate Normalizer mapping is compressed");
        options.put(REDUCED_RESPONSE, "Whether or not to return visibility markings on each attribute. Default: " + reducedResponse);
        options.put(Constants.RETURN_TYPE, "The method to use to serialize data for return to the client");
        options.put(FILTER_MASKED_VALUES, "Filter the masked values when both the masked and unmasked variants are in the result set.");
        options.put(INCLUDE_DATATYPE, "Include the data type as a field in the document.");
        options.put(DATATYPE_FIELDNAME, "The field name to use when inserting the fieldname into the document.");
        options.put(DATATYPE_FILTER, "CSV of data type names that should be included when scanning.");
        options.put(START_TIME, "The start time for this query in milliseconds");
        options.put(END_TIME, "The end time for this query in milliseconds");
        options.put(COLLECT_TIMING_DETAILS, "Collect timing details about the underlying iterators");

        return new IteratorOptions(getClass().getSimpleName(), "Runs a field-index only query against the DATAWAVE tables", options, null);
    }

    @Override
    public boolean validateOptions(Map<String,String> options) {
        if (log.isTraceEnabled()) {
            log.trace("Options: " + options);
        }

        // If we're not provided a query, we may not be performing any evaluation
        if (options.containsKey(QUERY)) {
            this.query = options.get(QUERY);
        } else if (!this.disableEvaluation) {
            log.error("If a query is not specified, evaluation must be disabled.");
            return false;
        }

        if (options.containsKey(QUERY_ID)) {
            this.queryId = options.get(QUERY_ID);
        }

        if (options.containsKey(QUERY_MAPPING_COMPRESS)) {
            compressedMappings = Boolean.valueOf(options.get(QUERY_MAPPING_COMPRESS));
        }

        this.validateTypeMetadata(options);

        // Currently writable, kryo or toString
        if (options.containsKey(Constants.RETURN_TYPE)) {
            setReturnType(ReturnType.valueOf(options.get(Constants.RETURN_TYPE)));
        }

        // Boolean: should each attribute maintain a ColumnVisibility.
        if (options.containsKey(REDUCED_RESPONSE)) {
            setReducedResponse(Boolean.parseBoolean(options.get(REDUCED_RESPONSE)));
        }

        this.getDocumentKey = GetStartKey.instance();
        this.mustUseFieldIndex = true;

        if (options.containsKey(FILTER_MASKED_VALUES)) {
            this.filterMaskedValues = Boolean.parseBoolean(options.get(FILTER_MASKED_VALUES));
        }

        if (options.containsKey(INCLUDE_DATATYPE)) {
            this.includeDatatype = Boolean.parseBoolean(options.get(INCLUDE_DATATYPE));
            if (this.includeDatatype) {
                this.datatypeKey = options.containsKey(DATATYPE_FIELDNAME) ? options.get(DATATYPE_FIELDNAME) : DEFAULT_DATATYPE_FIELDNAME;
            }
        }

        if (options.containsKey(DATATYPE_FILTER)) {
            String filterCsv = options.get(DATATYPE_FILTER);
            if (filterCsv != null && !filterCsv.isEmpty()) {
                HashSet<String> set = Sets.newHashSet(StringUtils.split(filterCsv, ','));
                Iterable<Text> tformed = Iterables.transform(set, new StringToText());
                if (options.containsKey(FI_NEXT_SEEK)) {
                    this.fieldIndexKeyDataTypeFilter = new FieldIndexKeyDataTypeFilter(tformed, getFiNextSeek());
                } else {
                    this.fieldIndexKeyDataTypeFilter = new FieldIndexKeyDataTypeFilter(tformed);
                }
                this.eventEntryKeyDataTypeFilter = new EventKeyDataTypeFilter(tformed);
            } else {
                this.fieldIndexKeyDataTypeFilter = KeyIdentity.Function;
                this.eventEntryKeyDataTypeFilter = KeyIdentity.Function;
            }
        } else {
            this.fieldIndexKeyDataTypeFilter = KeyIdentity.Function;
            this.eventEntryKeyDataTypeFilter = KeyIdentity.Function;
        }

        if (options.containsKey(INDEX_ONLY_FIELDS)) {
            this.indexOnlyFields = buildFieldSetFromString(options.get(INDEX_ONLY_FIELDS));
        } else if (!this.fullTableScanOnly) {
            log.error("A list of index only fields must be provided when running an optimized query");
            return false;
        }

        if (options.containsKey(INDEXED_FIELDS)) {
            this.indexedFields = buildFieldSetFromString(options.get(INDEXED_FIELDS));
        }

        if (options.containsKey(IGNORE_COLUMN_FAMILIES)) {
            this.ignoreColumnFamilies = buildIgnoredColumnFamilies(options.get(IGNORE_COLUMN_FAMILIES));
        }

        if (options.containsKey(START_TIME)) {
            this.startTime = Long.parseLong(options.get(START_TIME));
        } else {
            log.error("Must pass a value for " + START_TIME);
            return false;
        }

        if (options.containsKey(END_TIME)) {
            this.endTime = Long.parseLong(options.get(END_TIME));
        } else {
            log.error("Must pass a value for " + END_TIME);
            return false;
        }

        if (this.endTime < this.startTime) {
            log.error("The startTime was greater than the endTime: " + this.startTime + " > " + this.endTime);
            return false;
        }

        this.timeFilter = new TimeFilter(this.startTime, this.endTime);

        if (options.containsKey(COLLECT_TIMING_DETAILS)) {
            this.collectTimingDetails = Boolean.parseBoolean(options.get(COLLECT_TIMING_DETAILS));
        }

        return true;
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        if (log.isTraceEnabled()) {
            log.trace("QueryIterator init()");
        }

        if (!validateOptions(options)) {
            throw new IllegalArgumentException("Could not initialize QueryIterator with " + options);
        }

        this.documentOptions = options;
        this.myEnvironment = env;

        if (collectTimingDetails) {
            trackingSpan = new QuerySpan(getStatsdClient());
            this.source = new SourceTrackingIterator(trackingSpan, source);
        } else {
            this.source = source;
        }

        this.sourceForDeepCopies = this.source.deepCopy(this.myEnvironment);

    }

    protected void createAndSeekIndexIterator(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
                    throws IOException, ConfigException, IllegalAccessException, InstantiationException {
        boolean isQueryFullySatisfiedInitialState = true;
        String hitListOptionString = documentOptions.get("hit.list");

        if (hitListOptionString != null) {
            boolean hitListOption = Boolean.parseBoolean(hitListOptionString);
            if (hitListOption) {
                isQueryFullySatisfiedInitialState = false; // if hit list is on, don't attempt satisfiability
                // don't even make a SatisfactionVisitor.....
            }
        }
        Collection<String> unindexedTypes = Lists.newArrayList();

        Set<String> keys = fetchDataTypeKeys(this.documentOptions.get(NON_INDEXED_DATATYPES));

        String compressedOptionString = this.documentOptions.get(QUERY_MAPPING_COMPRESS);
        if (!org.apache.commons.lang3.StringUtils.isBlank(compressedOptionString)) {
            boolean compressedOption = Boolean.parseBoolean(compressedOptionString);
            if (compressedOption) {
                for (String key : keys) {
                    unindexedTypes.add(decompressOption(key, QueryOptions.UTF8));
                }
            }
        } else {
            unindexedTypes.addAll(keys);
        }

        if (isQueryFullySatisfiedInitialState) {
            SatisfactionVisitor satisfactionVisitor = this.createSatisfiabilityVisitor(true); // we'll charge in with optimism

            satisfactionVisitor.setUnindexedFields(unindexedTypes);
            // visit() and get the root which is the root of a tree of Boolean Logic Iterator<Key>'s
            this.getScript().jjtAccept(satisfactionVisitor, null);

            isQueryFullySatisfiedInitialState = satisfactionVisitor.isQueryFullySatisfied();

        }

        IteratorBuildingVisitor visitor = createIteratorBuildingVisitor(null, isQueryFullySatisfiedInitialState, sortedUIDs);

        visitor.setUnindexedFields(unindexedTypes);

        // visit() and get the root which is the root of a tree of Boolean Logic Iterator<Key>'s
        getScript().jjtAccept(visitor, null);
        NestedIterator<Key> root = visitor.root();

        if (null == root) {
            throw new UnindexedException("Could not instantiate iterators over field index for " + this.getQuery());
        } else {
            this.fieldIndexResults = new AccumuloFieldIndexIterable(root);
        }

        // Otherwise, we have to use the field index
        // Seek() the boolean logic stuff
        this.fieldIndexResults.seek(range, columnFamilies, inclusive);
    }

    public Iterator<Entry<Key,Document>> getDocumentIterator(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
                    throws IOException, ConfigException, InstantiationException, IllegalAccessException {
        createAndSeekIndexIterator(range, columnFamilies, inclusive);

        // Take the document Keys and transform it into Entry<Key,Document>, removing Attributes for this Document
        // which do not fall within the expected time range
        return this.fieldIndexResults.iterator();

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
        } catch (ConfigException e) {
            throw new IOException("Unable to create document iterator", e);
        } catch (IllegalAccessException e) {
            throw new IOException("Unable to create document iterator", e);
        } catch (InstantiationException e) {
            throw new IOException("Unable to create document iterator", e);
        }

        // Inject the data type as a field if the user requested it
        if (this.includeDatatype) {
            if (collectTimingDetails) {
                fieldIndexDocuments = Iterators.transform(fieldIndexDocuments,
                                new EvaluationTrackingFunction<>(QuerySpan.Stage.DataTypeAsField, trackingSpan, new DataTypeAsField(this.datatypeKey)));
            } else {
                fieldIndexDocuments = Iterators.transform(fieldIndexDocuments, new DataTypeAsField(this.datatypeKey));
            }
        }

        // Filter out masked values if requested
        if (this.filterMaskedValues) {
            // Should we filter here, or not?
        }

        if (collectTimingDetails) {
            // if there is no document to return, then add an empty document to
            // store the timing metadata using the documentRange endKey
            if (fieldIndexDocuments.hasNext() == false) {
                fieldIndexDocuments = Collections.singletonMap(this.range.getEndKey(), new Document()).entrySet().iterator();
            }
            fieldIndexDocuments = Iterators.transform(fieldIndexDocuments, new LogTiming(trackingSpan));
        }

        documentIterator = fieldIndexDocuments;

        prepareKeyValue();
    }

    /**
     * Get a FieldIndexAggregator
     *
     * @return a {@link IdentityAggregator}
     */
    @Override
    public FieldIndexAggregator getFiAggregator() {
        if (fiAggregator == null) {
            fiAggregator = new IdentityAggregator(null, null);
        }
        return fiAggregator;
    }
}
