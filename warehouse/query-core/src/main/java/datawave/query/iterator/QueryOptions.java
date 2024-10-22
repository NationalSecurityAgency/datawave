package datawave.query.iterator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.jexl3.JexlArithmetic;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.springframework.beans.FatalBeanException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.CharMatcher;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import datawave.core.iterators.DatawaveFieldIndexCachingIteratorJexl.HdfsBackedControl;
import datawave.core.iterators.filesystem.FileSystemCache;
import datawave.core.iterators.querylock.QueryLock;
import datawave.data.type.Type;
import datawave.ingest.data.config.ingest.CompositeIngest;
import datawave.query.Constants;
import datawave.query.DocumentSerialization;
import datawave.query.attributes.Document;
import datawave.query.attributes.ExcerptFields;
import datawave.query.attributes.UniqueFields;
import datawave.query.common.grouping.GroupFields;
import datawave.query.composite.CompositeMetadata;
import datawave.query.function.ConfiguredFunction;
import datawave.query.function.DocumentPermutation;
import datawave.query.function.Equality;
import datawave.query.function.GetStartKey;
import datawave.query.function.JexlEvaluation;
import datawave.query.function.PrefixEquality;
import datawave.query.function.serializer.DocumentSerializer;
import datawave.query.function.serializer.KryoDocumentSerializer;
import datawave.query.function.serializer.ToStringDocumentSerializer;
import datawave.query.function.serializer.WritableDocumentSerializer;
import datawave.query.iterator.filter.EventKeyDataTypeFilter;
import datawave.query.iterator.filter.FieldIndexKeyDataTypeFilter;
import datawave.query.iterator.filter.KeyIdentity;
import datawave.query.iterator.filter.StringToText;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.iterator.logic.IndexIterator;
import datawave.query.iterator.logic.TermFrequencyExcerptIterator;
import datawave.query.jexl.DefaultArithmetic;
import datawave.query.jexl.HitListArithmetic;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.FieldIndexAggregator;
import datawave.query.jexl.functions.IdentityAggregator;
import datawave.query.predicate.ConfiguredPredicate;
import datawave.query.predicate.EventDataQueryFieldFilter;
import datawave.query.predicate.EventDataQueryFilter;
import datawave.query.predicate.TimeFilter;
import datawave.query.statsd.QueryStatsDClient;
import datawave.query.tables.async.Scan;
import datawave.query.tracking.ActiveQueryLog;
import datawave.query.util.TypeMetadata;
import datawave.query.util.count.CountMap;
import datawave.query.util.count.CountMapSerDe;
import datawave.query.util.sortedset.FileSortedSet;
import datawave.util.StringUtils;
import datawave.util.UniversalSet;

/**
 * QueryOptions are set on the iterators.
 * <p>
 * Some options are passed through from the QueryParameters.
 */
public class QueryOptions implements OptionDescriber {
    private static final Logger log = Logger.getLogger(QueryOptions.class);

    public static final Charset UTF8 = StandardCharsets.UTF_8;

    public static final String DEBUG_MULTITHREADED_SOURCES = "debug.multithreaded.sources";

    public static final String SCAN_ID = Scan.SCAN_ID;
    public static final String DISABLE_EVALUATION = "disable.evaluation";
    public static final String DISABLE_FIELD_INDEX_EVAL = "disable.fi";
    public static final String LIMIT_SOURCES = "sources.limit.count";
    public static final String DISABLE_DOCUMENTS_WITHOUT_EVENTS = "disable.index.only.documents";
    public static final String QUERY = "query";
    public static final String QUERY_ID = "query.id";
    public static final String TYPE_METADATA = "type.metadata";
    public static final String TYPE_METADATA_AUTHS = "type.metadata.auths";
    public static final String METADATA_TABLE_NAME = "model.table.name";

    public static final String REDUCED_RESPONSE = "reduced.response";
    public static final String FULL_TABLE_SCAN_ONLY = "full.table.scan.only";

    public static final String PROJECTION_FIELDS = "projection.fields";
    public static final String DISALLOWLISTED_FIELDS = "disallowlisted.fields";
    public static final String INDEX_ONLY_FIELDS = "index.only.fields";
    public static final String INDEXED_FIELDS = "indexed.fields";
    public static final String COMPOSITE_FIELDS = "composite.fields";
    public static final String COMPOSITE_METADATA = "composite.metadata";
    public static final String COMPOSITE_SEEK_THRESHOLD = "composite.seek.threshold";
    public static final String CONTAINS_COMPOSITE_TERMS = "composite.terms";
    public static final String IGNORE_COLUMN_FAMILIES = "ignore.column.families";
    public static final String INCLUDE_GROUPING_CONTEXT = "include.grouping.context";
    public static final String DOCUMENT_PERMUTATION_CLASSES = "document.permutation.classes";
    public static final String TERM_FREQUENCY_FIELDS = "term.frequency.fields";
    public static final String TERM_FREQUENCIES_REQUIRED = "term.frequencies.are.required";
    public static final String CONTENT_EXPANSION_FIELDS = "content.expansion.fields";
    public static final String LIMIT_FIELDS = "limit.fields";
    public static final String MATCHING_FIELD_SETS = "matching.field.sets";
    public static final String LIMIT_FIELDS_PRE_QUERY_EVALUATION = "limit.fields.pre.query.evaluation";
    public static final String LIMIT_FIELDS_FIELD = "limit.fields.field";
    public static final String GROUP_FIELDS = "group.fields";
    public static final String GROUP_FIELDS_BATCH_SIZE = "group.fields.batch.size";
    public static final String UNIQUE_FIELDS = "unique.fields";
    public static final String HITS_ONLY = "hits.only";
    public static final String HIT_LIST = "hit.list";
    public static final String START_TIME = "start.time";
    public static final String END_TIME = "end.time";
    public static final String YIELD_THRESHOLD_MS = "yield.threshold.ms";

    public static final String FILTER_MASKED_VALUES = "filter.masked.values";
    public static final String INCLUDE_DATATYPE = "include.datatype";
    public static final String INCLUDE_RECORD_ID = "include.record.id";
    public static final String LOG_TIMING_DETAILS = "log.timing.details";
    public static final String COLLECT_TIMING_DETAILS = "collect.timing.details";
    public static final String STATSD_HOST_COLON_PORT = "statsd.host.colon.port";
    public static final String STATSD_MAX_QUEUE_SIZE = "statsd.max.queue.size";
    public static final String DATATYPE_FIELDNAME = "include.datatype.fieldname";
    public static final String TRACK_SIZES = "track.sizes";

    // pass through to Evaluating iterator to ensure consistency between query
    // logics

    // TODO: This DEFAULT_DATATYPE_FIELDNAME needs to be decided on
    public static final String DEFAULT_DATATYPE_FIELDNAME = "EVENT_DATATYPE";

    public static final String DEFAULT_PARENT_UID_FIELDNAME = Constants.PARENT_UID;

    public static final String DEFAULT_CHILD_COUNT_FIELDNAME = Constants.CHILD_COUNT;
    public static final String DEFAULT_DESCENDANT_COUNT_FIELDNAME = "DESCENDANT_COUNT";
    public static final String DEFAULT_HAS_CHILDREN_FIELDNAME = "HAS_CHILDREN";
    public static final String CHILD_COUNT_OUTPUT_IMMEDIATE_CHILDREN = "childcount.output.immediate";
    public static final String CHILD_COUNT_OUTPUT_ALL_DESCDENDANTS = "childcount.output.descendants";
    public static final String CHILD_COUNT_OUTPUT_HASCHILDREN = "childcount.output.haschildren";
    public static final String CHILD_COUNT_INDEX_DELIMITER = "childcount.index.delimiter";
    public static final String CHILD_COUNT_INDEX_FIELDNAME = "childcount.index.fieldname";
    public static final String CHILD_COUNT_INDEX_PATTERN = "childcount.index.pattern";
    public static final String CHILD_COUNT_INDEX_SKIP_THRESHOLD = "childcount.index.skip.threshold";

    public static final String INCLUDE_HIERARCHY_FIELDS = "include.hierarchy.fields";

    public static final String DATATYPE_FILTER = "datatype.filter";

    public static final String POSTPROCESSING_CLASSES = "postprocessing.classes";

    public static final String POSTPROCESSING_OPTIONS = "postprocessing.options";

    public static final String NON_INDEXED_DATATYPES = "non.indexed.dataTypes";

    public static final String EVERYTHING = "*";

    public static final String CONTAINS_INDEX_ONLY_TERMS = "contains.index.only.terms";

    public static final String ALLOW_FIELD_INDEX_EVALUATION = "allow.field.index.evaluation";

    public static final String ALLOW_TERM_FREQUENCY_LOOKUP = "allow.term.frequency.lookup";

    public static final String HDFS_SITE_CONFIG_URLS = "hdfs.site.config.urls";

    public static final String HDFS_FILE_COMPRESSION_CODEC = "hdfs.file.compression.codec";

    public static final String ZOOKEEPER_CONFIG = "zookeeper.config";

    public static final String IVARATOR_CACHE_DIR_CONFIG = "ivarator.cache.dir.config";

    public static final String IVARATOR_CACHE_BUFFER_SIZE = "ivarator.cache.buffer.size";

    public static final String IVARATOR_SCAN_PERSIST_THRESHOLD = "ivarator.scan.persist.threshold";

    public static final String IVARATOR_SCAN_TIMEOUT = "ivarator.scan.timeout";

    public static final String RESULT_TIMEOUT = "result.timeout";

    public static final String QUERY_MAPPING_COMPRESS = "query.mapping.compress";

    public static final String MAX_INDEX_RANGE_SPLIT = "max.index.range.split";

    public static final String MAX_IVARATOR_OPEN_FILES = "max.ivarator.open.files";

    public static final String IVARATOR_NUM_RETRIES = "ivarator.num.retries";

    public static final String IVARATOR_PERSIST_VERIFY = "ivarator.persist.verify";

    public static final String IVARATOR_PERSIST_VERIFY_COUNT = "ivarator.persist.verify.count";

    public static final String MAX_IVARATOR_SOURCES = "max.ivarator.sources";
    public static final String MAX_IVARATOR_SOURCE_WAIT = "max.ivarator.source.wait";

    public static final String MAX_IVARATOR_RESULTS = "max.ivarator.results";

    public static final String COMPRESS_SERVER_SIDE_RESULTS = "compress.server.side.results";

    public static final String MAX_EVALUATION_PIPELINES = "max.evaluation.pipelines";

    public static final String SERIAL_EVALUATION_PIPELINE = "serial.evaluation.pipeline";

    public static final String MAX_PIPELINE_CACHED_RESULTS = "max.pipeline.cached.results";

    public static final String DATE_INDEX_TIME_TRAVEL = "date.index.time.travel";

    public static final String SORTED_UIDS = "sorted.uids";

    public static final String RANGES = "ranges";

    /**
     * If a value is set, a separate {@link datawave.query.tracking.ActiveQueryLog} instance will be used instead of the shared default instance. The value is
     * typically a table name or query logic name.
     */
    public static final String ACTIVE_QUERY_LOG_NAME = "active.query.log.name";

    public static final String EXCERPT_FIELDS = "excerpt.fields";

    public static final String EXCERPT_FIELDS_NO_HIT_CALLOUT = "excerpt.fields.no.hit.callout";

    public static final String EXCERPT_ITERATOR = "excerpt.iterator.class";

    // field and next thresholds before a seek is issued
    public static final String FI_FIELD_SEEK = "fi.field.seek";
    public static final String FI_NEXT_SEEK = "fi.next.seek";
    public static final String EVENT_FIELD_SEEK = "event.field.seek";
    public static final String EVENT_NEXT_SEEK = "event.next.seek";
    public static final String TF_FIELD_SEEK = "tf.field.seek";
    public static final String TF_NEXT_SEEK = "tf.next.seek";

    public static final String SEEKING_EVENT_AGGREGATION = "seeking.event.aggregation";

    public static final String DOC_AGGREGATION_THRESHOLD_MS = "doc.agg.threshold";

    public static final String TERM_FREQUENCY_AGGREGATION_THRESHOLD_MS = "tf.agg.threshold";

    public static final String FIELD_COUNTS = "field.counts";
    public static final String TERM_COUNTS = "term.counts";

    protected Map<String,String> options;

    protected String scanId;
    protected String query;
    protected String queryId;
    protected boolean disableEvaluation = false;
    protected boolean disableFiEval = false;
    protected long sourceLimit = -1;
    protected boolean disableIndexOnlyDocuments = false;
    protected TypeMetadata typeMetadata = new TypeMetadata();
    protected Set<String> typeMetadataAuthsKey = Sets.newHashSet();
    protected CompositeMetadata compositeMetadata = null;
    protected int compositeSeekThreshold = 10;
    protected DocumentSerialization.ReturnType returnType = DocumentSerialization.ReturnType.kryo;
    private DocumentSerializer documentSerializer;

    protected boolean reducedResponse = false;
    protected boolean fullTableScanOnly = false;
    protected JexlArithmetic arithmetic = new DefaultArithmetic();

    protected boolean projectResults = false;
    protected boolean useAllowListedFields = false;
    protected Set<String> allowListedFields = new HashSet<>();
    protected boolean useDisallowListedFields = false;
    protected Set<String> disallowListedFields = new HashSet<>();
    protected Map<String,Integer> limitFieldsMap = new HashMap<>();
    protected Set<Set<String>> matchingFieldSets = new HashSet<>();
    protected boolean limitFieldsPreQueryEvaluation = false;
    protected String limitFieldsField = null;

    protected GroupFields groupFields = new GroupFields();
    protected int groupFieldsBatchSize = Integer.MAX_VALUE;
    protected UniqueFields uniqueFields = new UniqueFields();

    protected Set<String> hitsOnlySet = new HashSet<>();

    protected Function<Range,Key> getDocumentKey;

    protected FieldIndexAggregator fiAggregator;
    protected Equality equality;

    // filter for any key type (fi, event, tf)
    protected EventDataQueryFilter evaluationFilter;
    // filter specifically for event keys. required when performing a seeking aggregation
    protected EventDataQueryFilter eventFilter;

    protected int maxEvaluationPipelines = 25;
    protected int maxPipelineCachedResults = 25;

    protected Set<String> indexOnlyFields = Sets.newHashSet();
    protected Set<String> indexedFields = Sets.newHashSet();
    protected Set<String> ignoreColumnFamilies = Sets.newHashSet();

    protected boolean includeGroupingContext = false;

    protected List<String> documentPermutationClasses = new ArrayList<>();
    protected List<DocumentPermutation> documentPermutations = null;

    protected long startTime = 0L;
    protected long endTime = System.currentTimeMillis();
    protected TimeFilter timeFilter = null;

    // this flag control whether we filter the masked fields for results that
    // contain both the unmasked and masked variants. True by default.

    protected boolean filterMaskedValues = true;

    protected boolean includeRecordId = true;
    protected boolean includeDatatype = false;
    protected boolean includeHierarchyFields = false;
    protected String datatypeKey;
    protected boolean containsIndexOnlyTerms = false;
    protected boolean mustUseFieldIndex = false;

    protected boolean allowFieldIndexEvaluation = true;

    protected boolean allowTermFrequencyLookup = true;

    protected String hdfsSiteConfigURLs = null;
    protected String hdfsFileCompressionCodec = null;
    protected FileSystemCache fsCache = null;

    protected String zookeeperConfig = null;

    protected List<IvaratorCacheDirConfig> ivaratorCacheDirConfigs = Collections.emptyList();
    protected long ivaratorCacheScanPersistThreshold = 100000L;
    protected long ivaratorCacheScanTimeout = 1000L * 60 * 60;
    protected int ivaratorCacheBufferSize = 10000;

    protected long resultTimeout = 1000L * 60 * 60;
    protected int maxIndexRangeSplit = 11;
    protected int ivaratorMaxOpenFiles = 100;
    protected int ivaratorNumRetries = 2;
    protected FileSortedSet.PersistOptions ivaratorPersistOptions = new FileSortedSet.PersistOptions();

    protected int maxIvaratorSources = 33;
    protected long maxIvaratorSourceWait = 1000L * 60 * 30;

    protected long maxIvaratorResults = -1;

    protected long yieldThresholdMs = Long.MAX_VALUE;

    protected Predicate<Key> fieldIndexKeyDataTypeFilter = KeyIdentity.Function;
    protected Predicate<Key> eventEntryKeyDataTypeFilter = KeyIdentity.Function;

    protected String postProcessingFunctions = "";

    protected Map<String,Set<String>> nonIndexedDataTypeMap = Maps.newHashMap();

    protected boolean termFrequenciesRequired = false;
    protected Set<String> termFrequencyFields = Collections.emptySet();
    protected Set<String> contentExpansionFields;

    protected boolean compressResults = false;

    protected Boolean compressedMappings = false;

    // determine whether sortedUIDs are required. Normally they are, however if the query contains
    // only one indexed term, then there is no need to sort which can be a lot faster if an ivarator
    // is required.
    boolean sortedUIDs = true;

    protected boolean collectTimingDetails = false;

    protected String statsdHostAndPort = null;
    protected int statsdMaxQueueSize = 500;

    protected QueryStatsDClient statsdClient = null;

    protected boolean serialEvaluationPipeline = false;

    protected String metadataTableName;

    protected boolean dateIndexTimeTravel = false;

    protected boolean debugMultithreadedSources = false;

    /**
     * should document sizes be tracked
     */
    protected boolean trackSizes = true;

    /**
     * The name of the {@link datawave.query.tracking.ActiveQueryLog} instance to use.
     */
    protected String activeQueryLogName;

    protected ExcerptFields excerptFields;

    protected boolean excerptFieldsNoHitCallout;

    protected Class<? extends SortedKeyValueIterator<Key,Value>> excerptIterator = TermFrequencyExcerptIterator.class;

    // off by default, controls when to issue a seek
    private int fiFieldSeek = -1;
    private int fiNextSeek = -1;
    private int eventFieldSeek = -1;
    private int eventNextSeek = -1;
    private int tfFieldSeek = -1;
    private int tfNextSeek = -1;

    private boolean seekingEventAggregation = false;

    // aggregation thresholds
    private int docAggregationThresholdMs = -1;
    private int tfAggregationThresholdMs = -1;

    private CountMap fieldCounts;
    private CountMap termCounts;
    private CountMapSerDe mapSerDe;

    public void deepCopy(QueryOptions other) {
        this.options = other.options;
        this.query = other.query;
        this.queryId = other.queryId;
        this.scanId = other.scanId;
        this.disableEvaluation = other.disableEvaluation;
        this.disableIndexOnlyDocuments = other.disableIndexOnlyDocuments;
        this.typeMetadata = other.typeMetadata;
        this.typeMetadataAuthsKey = other.typeMetadataAuthsKey;
        this.metadataTableName = other.metadataTableName;
        this.compositeMetadata = other.compositeMetadata;
        this.compositeSeekThreshold = other.compositeSeekThreshold;
        this.returnType = other.returnType;
        this.reducedResponse = other.reducedResponse;
        this.fullTableScanOnly = other.fullTableScanOnly;

        this.projectResults = other.projectResults;
        this.useAllowListedFields = other.useAllowListedFields;
        this.allowListedFields = other.allowListedFields;
        this.useDisallowListedFields = other.useDisallowListedFields;
        this.disallowListedFields = other.disallowListedFields;

        this.fiAggregator = other.fiAggregator;

        this.indexOnlyFields = other.indexOnlyFields;
        this.indexedFields = other.indexedFields;
        this.ignoreColumnFamilies = other.ignoreColumnFamilies;

        this.includeGroupingContext = other.includeGroupingContext;

        this.documentPermutationClasses = other.documentPermutationClasses;
        this.documentPermutations = other.documentPermutations;

        this.startTime = other.startTime;
        this.endTime = other.endTime;
        this.timeFilter = other.timeFilter;

        this.filterMaskedValues = other.filterMaskedValues;
        this.includeDatatype = other.includeDatatype;
        this.datatypeKey = other.datatypeKey;
        this.includeRecordId = other.includeRecordId;

        this.includeHierarchyFields = other.includeHierarchyFields;

        this.fieldIndexKeyDataTypeFilter = other.fieldIndexKeyDataTypeFilter;
        this.eventEntryKeyDataTypeFilter = other.eventEntryKeyDataTypeFilter;

        this.postProcessingFunctions = other.postProcessingFunctions;

        this.nonIndexedDataTypeMap = other.nonIndexedDataTypeMap;

        this.containsIndexOnlyTerms = other.containsIndexOnlyTerms;

        this.getDocumentKey = other.getDocumentKey;
        this.equality = other.equality;
        this.evaluationFilter = other.evaluationFilter;

        this.ivaratorCacheDirConfigs = (other.ivaratorCacheDirConfigs == null) ? null : new ArrayList<>(other.ivaratorCacheDirConfigs);
        this.hdfsSiteConfigURLs = other.hdfsSiteConfigURLs;
        this.ivaratorCacheBufferSize = other.ivaratorCacheBufferSize;
        this.ivaratorCacheScanPersistThreshold = other.ivaratorCacheScanPersistThreshold;
        this.ivaratorCacheScanTimeout = other.ivaratorCacheScanTimeout;
        this.hdfsFileCompressionCodec = other.hdfsFileCompressionCodec;
        this.maxIndexRangeSplit = other.maxIndexRangeSplit;
        this.ivaratorMaxOpenFiles = other.ivaratorMaxOpenFiles;
        this.maxIvaratorSources = other.maxIvaratorSources;
        this.maxIvaratorSourceWait = other.maxIvaratorSourceWait;
        this.maxIvaratorResults = other.maxIvaratorResults;

        this.yieldThresholdMs = other.yieldThresholdMs;

        this.compressResults = other.compressResults;
        this.limitFieldsMap = other.limitFieldsMap;
        this.matchingFieldSets = other.matchingFieldSets;
        this.limitFieldsPreQueryEvaluation = other.limitFieldsPreQueryEvaluation;
        this.limitFieldsField = other.limitFieldsField;
        this.groupFields = other.groupFields;
        this.groupFieldsBatchSize = other.groupFieldsBatchSize;
        this.hitsOnlySet = other.hitsOnlySet;

        this.compressedMappings = other.compressedMappings;
        this.sortedUIDs = other.sortedUIDs;

        this.termFrequenciesRequired = other.termFrequenciesRequired;
        this.termFrequencyFields = other.termFrequencyFields;
        this.contentExpansionFields = other.contentExpansionFields;

        this.maxEvaluationPipelines = other.maxEvaluationPipelines;

        this.dateIndexTimeTravel = other.dateIndexTimeTravel;

        this.debugMultithreadedSources = other.debugMultithreadedSources;

        this.trackSizes = other.trackSizes;
        this.activeQueryLogName = other.activeQueryLogName;
        this.excerptFields = other.excerptFields;
        this.excerptFieldsNoHitCallout = other.excerptFieldsNoHitCallout;
        this.excerptIterator = other.excerptIterator;

        this.fiFieldSeek = other.fiFieldSeek;
        this.fiNextSeek = other.fiNextSeek;
        this.eventFieldSeek = other.eventFieldSeek;
        this.eventNextSeek = other.eventNextSeek;
        this.tfFieldSeek = other.tfFieldSeek;
        this.tfNextSeek = other.tfNextSeek;

        this.seekingEventAggregation = other.seekingEventAggregation;

        this.docAggregationThresholdMs = other.docAggregationThresholdMs;
        this.tfAggregationThresholdMs = other.tfAggregationThresholdMs;

        this.fieldCounts = other.fieldCounts;
        this.termCounts = other.termCounts;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getQueryId() {
        return queryId;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    public String getScanId() {
        return scanId;
    }

    public void setScanId(String scanId) {
        this.scanId = scanId;
    }

    public boolean isDisableEvaluation() {
        return disableEvaluation;
    }

    public void setDisableEvaluation(boolean disableEvaluation) {
        this.disableEvaluation = disableEvaluation;
    }

    public boolean disableIndexOnlyDocuments() {
        return disableIndexOnlyDocuments;
    }

    public void setDisableIndexOnlyDocuments(boolean disableIndexOnlyDocuments) {
        this.disableIndexOnlyDocuments = disableIndexOnlyDocuments;
    }

    public TypeMetadata getTypeMetadata() {
        // first, we will see it the query passed over the serialized TypeMetadata.
        // If it did, use that.
        if (this.typeMetadata != null && !this.typeMetadata.isEmpty()) {
            return this.typeMetadata;
        }
        log.debug("making a nothing typeMetadata");
        return new TypeMetadata();
    }

    public boolean isTrackSizes() {
        return trackSizes;
    }

    public void setTrackSizes(boolean trackSizes) {
        this.trackSizes = trackSizes;
    }

    public void setTypeMetadata(TypeMetadata typeMetadata) {
        this.typeMetadata = typeMetadata;
    }

    public CompositeMetadata getCompositeMetadata() {
        return compositeMetadata;
    }

    public void setCompositeMetadata(CompositeMetadata compositeMetadata) {
        this.compositeMetadata = compositeMetadata;
    }

    public int getCompositeSeekThreshold() {
        return compositeSeekThreshold;
    }

    public void setCompositeSeekThreshold(int compositeSeekThreshold) {
        this.compositeSeekThreshold = compositeSeekThreshold;
    }

    public DocumentSerialization.ReturnType getReturnType() {
        return returnType;
    }

    public void setReturnType(DocumentSerialization.ReturnType returnType) {
        this.returnType = returnType;
    }

    /**
     * Get the document serializer. If no serializer exists, create one based on the return type
     *
     * @return the document serializer
     */
    public DocumentSerializer getDocumentSerializer() {
        if (documentSerializer == null) {
            switch (returnType) {
                case kryo:
                    documentSerializer = new KryoDocumentSerializer(isReducedResponse(), isCompressResults());
                    break;
                case writable:
                    documentSerializer = new WritableDocumentSerializer(isReducedResponse());
                    break;
                case tostring:
                    documentSerializer = new ToStringDocumentSerializer(isReducedResponse());
                    break;
                case noop:
                default:
                    throw new IllegalArgumentException("Unknown return type of: " + returnType);
            }
        }
        return documentSerializer;
    }

    public void setDocumentSerializer(DocumentSerializer documentSerializer) {
        this.documentSerializer = documentSerializer;
    }

    public boolean isReducedResponse() {
        return reducedResponse;
    }

    public void setReducedResponse(boolean reducedResponse) {
        this.reducedResponse = reducedResponse;
    }

    public Predicate<Key> getFieldIndexKeyDataTypeFilter() {
        return this.fieldIndexKeyDataTypeFilter;
    }

    public Predicate<Key> getEventEntryKeyDataTypeFilter() {
        return this.eventEntryKeyDataTypeFilter;
    }

    public boolean isFullTableScanOnly() {
        return fullTableScanOnly;
    }

    public void setFullTableScanOnly(boolean fullTableScanOnly) {
        this.fullTableScanOnly = fullTableScanOnly;
    }

    public boolean isIncludeGroupingContext() {
        return includeGroupingContext;
    }

    public void setIncludeGroupingContext(boolean includeGroupingContext) {
        this.includeGroupingContext = includeGroupingContext;
    }

    public List<String> getDocumentPermutationClasses() {
        return documentPermutationClasses;
    }

    public List<DocumentPermutation> getDocumentPermutations() {
        if (documentPermutations == null) {
            List<DocumentPermutation> list = new ArrayList<>();
            TypeMetadata metadata = getTypeMetadata();
            for (String classname : getDocumentPermutationClasses()) {
                try {
                    Class<DocumentPermutation> clazz = (Class<DocumentPermutation>) Class.forName(classname);
                    try {
                        Constructor<DocumentPermutation> constructor = clazz.getConstructor(TypeMetadata.class);
                        list.add(constructor.newInstance(metadata));
                    } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
                        log.error("Unable to construct " + classname + " as a DocumentPermutation", e);
                        throw new IllegalArgumentException("Unable to construct " + classname + " as a DocumentPermutation", e);
                    } catch (NoSuchMethodException e) {
                        try {
                            list.add(clazz.newInstance());
                        } catch (InstantiationException | IllegalAccessException e2) {
                            log.error("Unable to construct " + classname + " as a DocumentPermutation", e2);
                            throw new IllegalArgumentException("Unable to construct " + classname + " as a DocumentPermutation", e2);
                        }
                    }
                } catch (ClassNotFoundException e) {
                    log.error("Unable to construct " + classname + " as a DocumentPermutation", e);
                    throw new IllegalArgumentException("Unable to construct " + classname + " as a DocumentPermutation", e);
                }
            }
            this.documentPermutations = list;
        }
        return this.documentPermutations;
    }

    public void setDocumentPermutationClasses(List<String> documentPermutationClasses) {
        this.documentPermutationClasses = documentPermutationClasses;
    }

    public void setDocumentPermutationClasses(String documentPermutationClassesStr) {
        setDocumentPermutationClasses(Arrays.asList(StringUtils.split(documentPermutationClassesStr, ',')));
    }

    public boolean isIncludeRecordId() {
        return includeRecordId;
    }

    public void setIncludeRecordId(boolean includeRecordId) {
        this.includeRecordId = includeRecordId;
    }

    public JexlArithmetic getArithmetic() {
        return arithmetic;
    }

    public void setArithmetic(JexlArithmetic arithmetic) {
        this.arithmetic = arithmetic;
    }

    /**
     * Gets a default implementation of a FieldIndexAggregator
     *
     * @return a FieldIndexAggregator
     */
    public FieldIndexAggregator getFiAggregator() {
        if (fiAggregator == null) {
            this.fiAggregator = new IdentityAggregator(getNonEventFields(), getEvaluationFilter(), getEventNextSeek());
        }
        return fiAggregator;
    }

    public EventDataQueryFilter getEvaluationFilter() {
        return evaluationFilter != null ? evaluationFilter.clone() : null;
    }

    public void setEvaluationFilter(EventDataQueryFilter evaluationFilter) {
        this.evaluationFilter = evaluationFilter;
    }

    /**
     * Return or build a field filter IFF this query is projecting results
     *
     * @return a field filter, or null if results are not projected
     */
    public EventDataQueryFilter getEventFilter() {

        if (!useAllowListedFields || allowListedFields instanceof UniversalSet || !isSeekingEventAggregation()) {
            return null;
        }

        if (eventFilter == null) {

            Set<String> fields = getEventFieldsToRetain();
            if (fields.contains(Constants.ANY_FIELD)) {
                return null;
            }

            //  @formatter:off
            eventFilter = new EventDataQueryFieldFilter()
                            .withFields(fields)
                            .withMaxNextCount(getEventNextSeek());
            //  @formatter:on
        }

        return eventFilter == null ? null : eventFilter.clone();
    }

    /**
     * Get the event fields to retain
     *
     * @return the set of event fields
     */
    private Set<String> getEventFieldsToRetain() {
        Set<String> fields = getQueryFields();

        if (!allowListedFields.isEmpty()) {
            fields.addAll(allowListedFields);
        }

        if (groupFields != null) {
            fields.addAll(groupFields.getGroupByFields());
        }

        if (!indexOnlyFields.isEmpty()) {
            // index only fields are not present in the event column
            fields.removeAll(indexOnlyFields);
        }

        // add composite components
        if (compositeMetadata != null && !compositeMetadata.isEmpty()) {
            Collection<Multimap<String,String>> entries = compositeMetadata.getCompositeFieldMapByType().values();
            for (Multimap<String,String> entry : entries) {
                fields.addAll(entry.values());
            }
        }

        return fields;
    }

    private Set<String> getQueryFields() {
        try {
            ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
            return JexlASTHelper.getIdentifierNames(script);
        } catch (ParseException e) {
            // ignore
            throw new FatalBeanException("Could not parse query");
        }
    }

    public TimeFilter getTimeFilter() {
        return timeFilter;
    }

    public void setTimeFilter(TimeFilter timeFilter) {
        this.timeFilter = timeFilter;
    }

    public Map<String,Set<String>> getNonIndexedDataTypeMap() {
        return nonIndexedDataTypeMap;
    }

    public void setNonIndexedDataTypeMap(Map<String,Set<String>> nonIndexedDataTypeMap) {
        this.nonIndexedDataTypeMap = nonIndexedDataTypeMap;
    }

    public Set<String> getIndexOnlyFields() {
        return this.indexOnlyFields;
    }

    public Set<String> getIndexedFields() {
        return this.indexedFields;
    }

    public Set<String> getAllIndexOnlyFields() {
        Set<String> allIndexOnlyFields = new HashSet<>();
        // index only fields are by definition not in the event
        if (indexOnlyFields != null) {
            allIndexOnlyFields.addAll(indexOnlyFields);
        }
        // composite fields are index only as well, unless they are overloaded composites
        if (compositeMetadata != null) {
            for (Multimap<String,String> compositeFieldMap : compositeMetadata.getCompositeFieldMapByType().values()) {
                for (String compositeField : compositeFieldMap.keySet()) {
                    if (!CompositeIngest.isOverloadedCompositeField(compositeFieldMap, compositeField)) {
                        allIndexOnlyFields.add(compositeField);
                    }
                }
            }
        }
        return allIndexOnlyFields;
    }

    /**
     * Get the fields that contain data that may not be in the event
     *
     * @return a set of event fields
     */
    public Set<String> getNonEventFields() {
        Set<String> nonEventFields = new HashSet<>();
        // index only fields are by definition not in the event
        if (indexOnlyFields != null) {
            nonEventFields.addAll(indexOnlyFields);
        }
        // term frequency fields contain forms of the data (tokens) that are not in the event in the same form
        if (termFrequencyFields != null) {
            nonEventFields.addAll(termFrequencyFields);
        }
        // composite metadata contains combined fields that are not in the event in the same form
        if (compositeMetadata != null) {
            for (Multimap<String,String> compositeFieldMap : compositeMetadata.getCompositeFieldMapByType().values()) {
                for (String compositeField : compositeFieldMap.keySet()) {
                    if (!CompositeIngest.isOverloadedCompositeField(compositeFieldMap, compositeField)) {
                        nonEventFields.add(compositeField);
                    }
                }
            }
        }
        return nonEventFields;
    }

    /**
     * Get the union of all fields set via the following QueryOptions
     * <ul>
     * <li>{@link #INDEXED_FIELDS}</li>
     * <li>{@link #INDEX_ONLY_FIELDS}</li>
     * <li>{@link #TERM_FREQUENCY_FIELDS}</li>
     * <li>{@link #COMPOSITE_FIELDS}</li>
     * <li>{@link #CONTENT_EXPANSION_FIELDS}</li>
     * </ul>
     *
     * @return the union of all configured fields
     */
    public Set<String> getAllFields() {
        Set<String> allFields = new HashSet<>();
        // includes index only fields plus composite fields
        allFields.addAll(getAllIndexOnlyFields());
        // should be a subset of tf fields
        allFields.addAll(getContentExpansionFields());
        allFields.addAll(getIndexedFields());
        allFields.addAll(getTermFrequencyFields());
        // also grab non-indexed fields
        allFields.addAll(getNonIndexedDataTypeMap().keySet());
        return allFields;
    }

    public boolean isContainsIndexOnlyTerms() {
        return containsIndexOnlyTerms;
    }

    public void setContainsIndexOnlyTerms(boolean containsIndexOnlyTerms) {
        this.containsIndexOnlyTerms = containsIndexOnlyTerms;
    }

    public boolean isAllowFieldIndexEvaluation() {
        return allowFieldIndexEvaluation;
    }

    public void setAllowFieldIndexEvaluation(boolean allowFieldIndexEvaluation) {
        this.allowFieldIndexEvaluation = allowFieldIndexEvaluation;
    }

    public boolean isAllowTermFrequencyLookup() {
        return allowTermFrequencyLookup;
    }

    public void setAllowTermFrequencyLookup(boolean allowTermFrequencyLookup) {
        this.allowTermFrequencyLookup = allowTermFrequencyLookup;
    }

    public String getHdfsSiteConfigURLs() {
        return hdfsSiteConfigURLs;
    }

    public void setHdfsSiteConfigURLs(String hadoopConfigURLs) {
        this.hdfsSiteConfigURLs = hadoopConfigURLs;
    }

    public FileSystemCache getFileSystemCache() throws MalformedURLException {
        if (this.fsCache == null && this.hdfsSiteConfigURLs != null) {
            this.fsCache = new FileSystemCache(this.hdfsSiteConfigURLs);
        }
        return this.fsCache;
    }

    public QueryLock getQueryLock() throws MalformedURLException, ConfigException {
        return new QueryLock.Builder().forQueryId(getQueryId()).forFSCache(getFileSystemCache())
                        .forIvaratorDirs(ivaratorCacheDirConfigs.stream().map(IvaratorCacheDirConfig::getBasePathURI).collect(Collectors.joining(",")))
                        .forZookeeper(getZookeeperConfig(), HdfsBackedControl.CANCELLED_CHECK_INTERVAL * 2).build();
    }

    public String getHdfsFileCompressionCodec() {
        return hdfsFileCompressionCodec;
    }

    public void setHdfsFileCompressionCodec(String hdfsFileCompressionCodec) {
        this.hdfsFileCompressionCodec = hdfsFileCompressionCodec;
    }

    public String getZookeeperConfig() {
        return zookeeperConfig;
    }

    public void setZookeeperConfig(String zookeeperConfig) {
        this.zookeeperConfig = zookeeperConfig;
    }

    public List<IvaratorCacheDirConfig> getIvaratorCacheDirConfigs() {
        return ivaratorCacheDirConfigs;
    }

    public void setIvaratorCacheDirConfigs(List<IvaratorCacheDirConfig> ivaratorCacheDirConfigs) {
        this.ivaratorCacheDirConfigs = ivaratorCacheDirConfigs;
    }

    public int getIvaratorCacheBufferSize() {
        return ivaratorCacheBufferSize;
    }

    public void setIvaratorCacheBufferSize(int ivaratorCacheBufferSize) {
        this.ivaratorCacheBufferSize = ivaratorCacheBufferSize;
    }

    public long getIvaratorCacheScanPersistThreshold() {
        return ivaratorCacheScanPersistThreshold;
    }

    public void setIvaratorCacheScanPersistThreshold(long ivaratorCacheScanPersistThreshold) {
        this.ivaratorCacheScanPersistThreshold = ivaratorCacheScanPersistThreshold;
    }

    public long getIvaratorCacheScanTimeout() {
        return ivaratorCacheScanTimeout;
    }

    public void setIvaratorCacheScanTimeout(long ivaratorCacheScanTimeout) {
        this.ivaratorCacheScanTimeout = ivaratorCacheScanTimeout;
    }

    public long getResultTimeout() {
        return resultTimeout;
    }

    public void setResultTimeout(long resultTimeout) {
        this.resultTimeout = resultTimeout;
    }

    public int getMaxIndexRangeSplit() {
        return maxIndexRangeSplit;
    }

    public void setMaxIndexRangeSplit(int maxIndexRangeSplit) {
        this.maxIndexRangeSplit = maxIndexRangeSplit;
    }

    public int getIvaratorMaxOpenFiles() {
        return ivaratorMaxOpenFiles;
    }

    public void setIvaratorMaxOpenFiles(int ivaratorMaxOpenFiles) {
        this.ivaratorMaxOpenFiles = ivaratorMaxOpenFiles;
    }

    public int getIvaratorNumRetries() {
        return ivaratorNumRetries;
    }

    public void setIvaratorNumRetries(int ivaratorNumRetries) {
        this.ivaratorNumRetries = ivaratorNumRetries;
    }

    public FileSortedSet.PersistOptions getIvaratorPersistOptions() {
        return ivaratorPersistOptions;
    }

    public void setIvaratorPersistOptions(FileSortedSet.PersistOptions ivaratorPersistOptions) {
        this.ivaratorPersistOptions = ivaratorPersistOptions;
    }

    public int getMaxIvaratorSources() {
        return maxIvaratorSources;
    }

    public void setMaxIvaratorSources(int maxIvaratorSources) {
        this.maxIvaratorSources = maxIvaratorSources;
    }

    public long getMaxIvaratorSourceWait() {
        return maxIvaratorSourceWait;
    }

    public void setMaxIvaratorSourceWait(long maxIvaratorSourceWait) {
        this.maxIvaratorSourceWait = maxIvaratorSourceWait;
    }

    public long getMaxIvaratorResults() {
        return maxIvaratorResults;
    }

    public void setMaxIvaratorResults(long maxIvaratorResults) {
        this.maxIvaratorResults = maxIvaratorResults;
    }

    public boolean isCompressResults() {
        return compressResults;
    }

    public void setCompressResults(boolean compressResults) {
        this.compressResults = compressResults;
    }

    public Map<String,Integer> getLimitFieldsMap() {
        return limitFieldsMap;
    }

    public void setLimitFieldsMap(Map<String,Integer> limitFieldsMap) {
        this.limitFieldsMap = limitFieldsMap;
    }

    public Set<Set<String>> getMatchingFieldSets() {
        return matchingFieldSets;
    }

    public List<String> getMatchingFieldList() {
        return this.matchingFieldSets.stream().flatMap(s -> s.stream()).collect(Collectors.toList());
    }

    public void setMatchingFieldSets(Set<Set<String>> matchingFieldSets) {
        this.matchingFieldSets = matchingFieldSets;
    }

    public boolean isLimitFieldsPreQueryEvaluation() {
        return limitFieldsPreQueryEvaluation;
    }

    public void setLimitFieldsPreQueryEvaluation(boolean limitFieldsPreQueryEvaluation) {
        this.limitFieldsPreQueryEvaluation = limitFieldsPreQueryEvaluation;
    }

    public String getLimitFieldsField() {
        return limitFieldsField;
    }

    public void setLimitFieldsField(String limitFieldsField) {
        this.limitFieldsField = limitFieldsField;
    }

    public GroupFields getGroupFields() {
        return groupFields;
    }

    public void setGroupFields(GroupFields groupFields) {
        this.groupFields = groupFields;
    }

    public int getGroupFieldsBatchSize() {
        return groupFieldsBatchSize;
    }

    public void setGroupFieldsBatchSize(int groupFieldsBatchSize) {
        this.groupFieldsBatchSize = groupFieldsBatchSize;
    }

    public UniqueFields getUniqueFields() {
        return uniqueFields;
    }

    public void setUniqueFields(UniqueFields uniqueFields) {
        this.uniqueFields = uniqueFields;
    }

    public Set<String> getHitsOnlySet() {
        return hitsOnlySet;
    }

    public void setHitsOnlySet(Set<String> hitsOnlySet) {
        this.hitsOnlySet = hitsOnlySet;
    }

    public boolean isDateIndexTimeTravel() {
        return dateIndexTimeTravel;
    }

    public void setDateIndexTimeTravel(boolean dateIndexTimeTravel) {
        this.dateIndexTimeTravel = dateIndexTimeTravel;
    }

    public boolean isSortedUIDs() {
        return sortedUIDs;
    }

    public void setSortedUIDs(boolean sortedUIDs) {
        this.sortedUIDs = sortedUIDs;
    }

    public boolean isDebugMultithreadedSources() {
        return debugMultithreadedSources;
    }

    public void setDebugMultithreadedSources(boolean debugMultithreadedSources) {
        this.debugMultithreadedSources = debugMultithreadedSources;
    }

    public String getActiveQueryLogName() {
        return activeQueryLogName;
    }

    public void setActiveQueryLogName(String activeQueryLogName) {
        this.activeQueryLogName = activeQueryLogName;
    }

    public ExcerptFields getExcerptFields() {
        return excerptFields;
    }

    public void setExcerptFields(ExcerptFields excerptFields) {
        this.excerptFields = excerptFields;
    }

    public boolean getExcerptFieldsNoHitCallout() {
        return excerptFieldsNoHitCallout;
    }

    public void setExcerptFieldsNoHitCallout(boolean excerptFieldsNoHitCallout) {
        this.excerptFieldsNoHitCallout = excerptFieldsNoHitCallout;
    }

    public Class<? extends SortedKeyValueIterator<Key,Value>> getExcerptIterator() {
        return excerptIterator;
    }

    public void setExcerptIterator(Class<? extends SortedKeyValueIterator<Key,Value>> excerptIterator) {
        this.excerptIterator = excerptIterator;
    }

    @Override
    public IteratorOptions describeOptions() {
        Map<String,String> options = new HashMap<>();

        options.put(DISABLE_EVALUATION, "If provided, JEXL evaluation is not performed against any document.");
        options.put(DISABLE_FIELD_INDEX_EVAL,
                        "If provided, a query tree is not evaluated against the field index. Only used in the case of doc specific ranges");
        options.put(LIMIT_SOURCES, "Allows client to limit the number of sources used for this scan");
        options.put(DISABLE_DOCUMENTS_WITHOUT_EVENTS, "Removes documents in which only hits against the index were found, and no event");
        options.put(QUERY, "The JEXL query to evaluate documents against");
        options.put(QUERY_ID, "The UUID of the query");
        options.put(TYPE_METADATA, "A mapping of field name to a set of DataType class names");
        options.put(QUERY_MAPPING_COMPRESS, "Boolean value to indicate Normalizer mapping is compressed");
        options.put(REDUCED_RESPONSE, "Whether or not to return visibility markings on each attribute. Default: " + reducedResponse);
        options.put(Constants.RETURN_TYPE, "The method to use to serialize data for return to the client");
        options.put(FULL_TABLE_SCAN_ONLY, "If true, do not perform boolean logic, just scan the documents");
        options.put(PROJECTION_FIELDS, "Attributes to return to the client");
        options.put(DISALLOWLISTED_FIELDS, "Attributes to *not* return to the client");
        options.put(FILTER_MASKED_VALUES, "Filter the masked values when both the masked and unmasked variants are in the result set.");
        options.put(INCLUDE_DATATYPE, "Include the data type as a field in the document.");
        options.put(INCLUDE_RECORD_ID, "Include the record id as a field in the document.");
        options.put(COLLECT_TIMING_DETAILS, "Collect timing details about the underlying iterators");
        options.put(STATSD_HOST_COLON_PORT,
                        "A configured statsd host:port which will be used to send resource and timing details from the underlying iterators if configured");
        options.put(STATSD_MAX_QUEUE_SIZE, "Max queued metrics before statsd metrics are flushed");
        options.put(INCLUDE_HIERARCHY_FIELDS, "Include the hierarchy fields (CHILD_COUNT and PARENT_UID) as document fields.");
        options.put(DATATYPE_FIELDNAME, "The field name to use when inserting the fieldname into the document.");
        options.put(DATATYPE_FILTER, "CSV of data type names that should be included when scanning.");
        options.put(INDEX_ONLY_FIELDS, "The serialized collection of field names that only occur in the index");
        options.put(INDEXED_FIELDS, "The serialized collection of indexed fields.");
        options.put(COMPOSITE_FIELDS, "The serialized collection of field names that make up composites");
        options.put(START_TIME, "The start time for this query in milliseconds");
        options.put(END_TIME, "The end time for this query in milliseconds");
        options.put(POSTPROCESSING_CLASSES, "CSV of functions and predicates to apply to documents that pass the original query.");
        options.put(IndexIterator.INDEX_FILTERING_CLASSES, "CSV of predicates to apply to keys that pass the original field index (fi) scan.");
        options.put(INCLUDE_GROUPING_CONTEXT, "Keep the grouping context on the final returned document");
        options.put(DOCUMENT_PERMUTATION_CLASSES,
                        "Classes implementing DocumentPermutation which can transform the document prior to evaluation (e.g. expand/mutate fields).");
        options.put(LIMIT_FIELDS, "limit fields");
        options.put(MATCHING_FIELD_SETS, "matching field sets (used along with limit fields)");
        options.put(GROUP_FIELDS, "group fields and fields to aggregate");
        options.put(GROUP_FIELDS_BATCH_SIZE, "group fields.batch.size");
        options.put(UNIQUE_FIELDS, "unique fields");
        options.put(HIT_LIST, "hit list");
        options.put(NON_INDEXED_DATATYPES, "Normalizers to apply only at aggregation time");
        options.put(CONTAINS_INDEX_ONLY_TERMS, "Does the query being evaluated contain any terms which are index-only");
        options.put(ALLOW_FIELD_INDEX_EVALUATION,
                        "Allow the evaluation to occur purely on values pulled from the field index for queries only accessing indexed fields (default is true)");
        options.put(ALLOW_TERM_FREQUENCY_LOOKUP, "Allow the evaluation to use the term frequencies in lieu of the field index when appropriate");
        options.put(TERM_FREQUENCIES_REQUIRED, "Does the query require gathering term frequencies");
        options.put(TERM_FREQUENCY_FIELDS, "comma-delimited list of fields that contain term frequencies");
        options.put(CONTENT_EXPANSION_FIELDS, "comma-delimited list of fields used for content function expansions");
        options.put(HDFS_SITE_CONFIG_URLS, "URLs (comma delimited) of where to find the hadoop hdfs and core site configuration files");
        options.put(HDFS_FILE_COMPRESSION_CODEC, "A hadoop compression codec to use for files if supported");
        options.put(IVARATOR_CACHE_DIR_CONFIG,
                        "A JSON-formatted array of ivarator cache config objects.  Each config object MUST specify a pathURI to use when caching field index iterator output.");
        options.put(IVARATOR_CACHE_BUFFER_SIZE, "The size of the hdfs cache buffer size (items held in memory before dumping to hdfs).  Default is 10000.");
        options.put(IVARATOR_SCAN_PERSIST_THRESHOLD,
                        "The number of underlying field index keys scanned before the hdfs cache buffer is forced to persist).  Default is 100000.");
        options.put(IVARATOR_SCAN_TIMEOUT, "The time after which the hdfs cache buffer is forced to persist.  Default is 60 minutes.");
        options.put(RESULT_TIMEOUT, "The time out after which an intermediate result is returned for a groupby or unique query.  Default is 60 minutes.");
        options.put(MAX_INDEX_RANGE_SPLIT,
                        "The maximum number of ranges to split a field index scan (ivarator) range into for multithreading.  Note the thread pool size is controlled via an accumulo property.");
        options.put(MAX_IVARATOR_OPEN_FILES,
                        "The maximum number of files that can be opened at one time during a merge sort.  If more that this number of files are created, then compactions will occur");
        options.put(IVARATOR_NUM_RETRIES,
                        "The number of times an ivarator should attempt to persist a sorted set to a given ivarator cache directory.  We will use the specified number of retries for each of the configured ivarator cache directories.");
        options.put(MAX_IVARATOR_SOURCES,
                        " The maximum number of sources to use for ivarators across all ivarated terms within the query.  Note the thread pool size is controlled via an accumulo property.");
        options.put(YIELD_THRESHOLD_MS,
                        "The threshold in milliseconds that the query iterator will evaluate consecutive documents to false before yielding the scan.");
        options.put(COMPRESS_SERVER_SIDE_RESULTS, "GZIP compress the serialized Documents before returning to the webserver");
        options.put(MAX_EVALUATION_PIPELINES, "The max number of evaluation pipelines");
        options.put(SERIAL_EVALUATION_PIPELINE, "Forces us to use the serial pipeline. Allows us to still have a single thread for evaluation");
        options.put(MAX_PIPELINE_CACHED_RESULTS, "The max number of non-null evaluated results to cache beyond the evaluation pipelines in queue");
        options.put(DATE_INDEX_TIME_TRAVEL, "Whether the shards from before the event should be gathered from the dateIndex");

        options.put(SORTED_UIDS,
                        "Whether the UIDs need to be sorted.  Normally this is true, however in limited circumstances it could be false which allows ivarators to avoid pre-fetching all UIDs and sorting before returning the first one.");

        options.put(RANGES, "The ranges associated with this scan.  Intended to be used for investigative purposes.");

        options.put(DEBUG_MULTITHREADED_SOURCES, "If provided, the SourceThreadTrackingIterator will be used");

        options.put(METADATA_TABLE_NAME, this.metadataTableName);
        options.put(LIMIT_FIELDS_PRE_QUERY_EVALUATION, "If true, non-query fields limits will be applied immediately off the iterator");
        options.put(LIMIT_FIELDS_FIELD,
                        "When " + LIMIT_FIELDS_PRE_QUERY_EVALUATION + " is set to true this field will contain all fields that were limited immediately");
        options.put(ACTIVE_QUERY_LOG_NAME, "If not provided or set to '" + ActiveQueryLog.DEFAULT_NAME
                        + "', will use the default shared Active Query Log instance. If provided otherwise, uses a separate distinct Active Query Log that will include the unique name in log messages.");
        options.put(EXCERPT_FIELDS, "excerpt fields");
        options.put(EXCERPT_FIELDS_NO_HIT_CALLOUT, "excerpt fields no hit callout");
        options.put(EXCERPT_ITERATOR, "excerpt iterator class (default datawave.query.iterator.logic.TermFrequencyExcerptIterator");
        options.put(FI_FIELD_SEEK, "The number of fields traversed by a Field Index data filter or aggregator before a seek is issued");
        options.put(FI_NEXT_SEEK, "The number of next calls made by a Field Index data filter or aggregator before a seek is issued");
        options.put(EVENT_FIELD_SEEK, "The number of fields traversed by an Event data filter or aggregator before a seek is issued");
        options.put(EVENT_NEXT_SEEK, "The number of next calls made by an Event data filter or aggregator before a seek is issued");
        options.put(TF_FIELD_SEEK, "The number of fields traversed by a Term Frequency data filter or aggregator before a seek is issued");
        options.put(TF_NEXT_SEEK, "The number of next calls made by a Term Frequency data filter or aggregator before a seek is issued");
        options.put(DOC_AGGREGATION_THRESHOLD_MS, "Document aggregations that exceed this threshold are logged as a warning");
        options.put(TERM_FREQUENCY_AGGREGATION_THRESHOLD_MS, "TermFrequency aggregations that exceed this threshold are logged as a warning");
        options.put(FIELD_COUNTS, "Map of field counts from the global index");
        options.put(TERM_COUNTS, "Map of term counts from the global index");
        return new IteratorOptions(getClass().getSimpleName(), "Runs a query against the DATAWAVE tables", options, null);
    }

    @Override
    public boolean validateOptions(Map<String,String> options) {
        if (log.isTraceEnabled()) {
            log.trace("Options: " + options);
        }

        this.options = options;

        // If we don't have a query, make sure it's because
        // we aren't performing any Jexl evaluation
        if (options.containsKey(DISABLE_EVALUATION)) {
            this.disableEvaluation = Boolean.parseBoolean(options.get(DISABLE_EVALUATION));
        }

        if (options.containsKey(DISABLE_FIELD_INDEX_EVAL)) {
            this.disableFiEval = Boolean.parseBoolean(options.get(DISABLE_FIELD_INDEX_EVAL));
        }

        if (options.containsKey(LIMIT_SOURCES)) {
            try {
                this.sourceLimit = Long.parseLong(options.get(LIMIT_SOURCES));
            } catch (NumberFormatException nfe) {
                this.sourceLimit = -1;
            }
        }

        if (options.containsKey(DISABLE_DOCUMENTS_WITHOUT_EVENTS)) {
            this.disableIndexOnlyDocuments = Boolean.parseBoolean(options.get(DISABLE_DOCUMENTS_WITHOUT_EVENTS));
        }

        // If we're not provided a query, we may not be performing any
        // evaluation
        if (options.containsKey(QUERY)) {
            this.query = options.get(QUERY);
        } else if (!this.disableEvaluation) {
            log.error("If a query is not specified, evaluation must be disabled.");
            return false;
        }

        if (options.containsKey(QUERY_ID)) {
            this.queryId = options.get(QUERY_ID);
        }

        if (options.containsKey(SCAN_ID)) {
            this.scanId = options.get(SCAN_ID);
        }

        if (options.containsKey(QUERY_MAPPING_COMPRESS)) {
            compressedMappings = Boolean.valueOf(options.get(QUERY_MAPPING_COMPRESS));
        }

        this.validateTypeMetadata(options);

        if (options.containsKey(COMPOSITE_METADATA)) {
            String compositeMetadataString = options.get(COMPOSITE_METADATA);
            if (compositeMetadataString != null && !compositeMetadataString.isEmpty()) {
                this.compositeMetadata = CompositeMetadata.fromBytes(java.util.Base64.getDecoder().decode(compositeMetadataString));
            }

            if (log.isTraceEnabled()) {
                log.trace("Using compositeMetadata: " + this.compositeMetadata);
            }
        }

        if (options.containsKey(COMPOSITE_SEEK_THRESHOLD)) {
            try {
                this.compositeSeekThreshold = Integer.parseInt(options.get(COMPOSITE_SEEK_THRESHOLD));
            } catch (NumberFormatException nfe) {
                this.compositeSeekThreshold = 10;
            }
        }

        // Currently writable, kryo or toString
        if (options.containsKey(Constants.RETURN_TYPE)) {
            setReturnType(DocumentSerialization.ReturnType.valueOf(options.get(Constants.RETURN_TYPE)));
        }

        // Boolean: should each attribute maintain a ColumnVisibility.
        if (options.containsKey(REDUCED_RESPONSE)) {
            setReducedResponse(Boolean.parseBoolean(options.get(REDUCED_RESPONSE)));
        }

        if (options.containsKey(FULL_TABLE_SCAN_ONLY)) {
            setFullTableScanOnly(Boolean.parseBoolean(options.get(FULL_TABLE_SCAN_ONLY)));
        }

        if (options.containsKey(TRACK_SIZES) && options.get(TRACK_SIZES) != null) {
            setTrackSizes(Boolean.parseBoolean(options.get(TRACK_SIZES)));
        }

        if (options.containsKey(PROJECTION_FIELDS)) {
            this.projectResults = true;
            this.useAllowListedFields = true;

            String fieldList = options.get(PROJECTION_FIELDS);
            if (fieldList != null && EVERYTHING.equals(fieldList)) {
                this.allowListedFields = UniversalSet.instance();
            } else if (fieldList != null && !fieldList.trim().equals("")) {
                this.allowListedFields = new HashSet<>();
                Collections.addAll(this.allowListedFields, StringUtils.split(fieldList, Constants.PARAM_VALUE_SEP));
            }
            if (options.containsKey(HIT_LIST) && Boolean.parseBoolean(options.get(HIT_LIST))) {
                this.allowListedFields.add(JexlEvaluation.HIT_TERM_FIELD);
            }
        }

        if (options.containsKey(DISALLOWLISTED_FIELDS)) {
            if (this.projectResults) {
                log.error("QueryOptions.PROJECTION_FIELDS and QueryOptions.DISALLOWLISTED_FIELDS are mutually exclusive");
                return false;
            }

            this.projectResults = true;
            this.useDisallowListedFields = true;

            String fieldList = options.get(DISALLOWLISTED_FIELDS);
            if (fieldList != null && !fieldList.trim().equals("")) {
                this.disallowListedFields = new HashSet<>();
                Collections.addAll(this.disallowListedFields, StringUtils.split(fieldList, Constants.PARAM_VALUE_SEP));
            }
        }

        if (options.containsKey(FIELD_COUNTS)) {
            String serializedMap = options.get(FIELD_COUNTS);
            this.fieldCounts = getMapSerDe().deserializeFromString(serializedMap);
        }

        if (options.containsKey(TERM_COUNTS)) {
            String serializedMap = options.get(TERM_COUNTS);
            this.termCounts = getMapSerDe().deserializeFromString(serializedMap);
        }

        this.evaluationFilter = null;
        this.getDocumentKey = GetStartKey.instance();
        this.mustUseFieldIndex = false;

        if (options.containsKey(FILTER_MASKED_VALUES)) {
            this.filterMaskedValues = Boolean.parseBoolean(options.get(FILTER_MASKED_VALUES));
        }

        if (options.containsKey(INCLUDE_DATATYPE)) {
            this.includeDatatype = Boolean.parseBoolean(options.get(INCLUDE_DATATYPE));
            if (this.includeDatatype) {
                this.datatypeKey = options.getOrDefault(DATATYPE_FIELDNAME, DEFAULT_DATATYPE_FIELDNAME);
            }
        }

        if (options.containsKey(INCLUDE_RECORD_ID)) {
            this.includeRecordId = Boolean.parseBoolean(options.get(INCLUDE_RECORD_ID));
        }

        if (options.containsKey(COLLECT_TIMING_DETAILS)) {
            this.collectTimingDetails = Boolean.parseBoolean(options.get(COLLECT_TIMING_DETAILS));
        }

        if (options.containsKey(STATSD_HOST_COLON_PORT)) {
            this.statsdHostAndPort = options.get(STATSD_HOST_COLON_PORT);
        }

        if (options.containsKey(STATSD_MAX_QUEUE_SIZE)) {
            this.statsdMaxQueueSize = Integer.parseInt(options.get(STATSD_MAX_QUEUE_SIZE));
        }

        if (options.containsKey(INCLUDE_HIERARCHY_FIELDS)) {
            this.includeHierarchyFields = Boolean.parseBoolean(options.get(INCLUDE_HIERARCHY_FIELDS));
        }

        // parse seek thresholds before building filters/aggregators
        if (options.containsKey(FI_FIELD_SEEK)) {
            this.fiFieldSeek = Integer.parseInt(options.get(FI_FIELD_SEEK));
        }

        if (options.containsKey(FI_NEXT_SEEK)) {
            this.fiNextSeek = Integer.parseInt(options.get(FI_NEXT_SEEK));
        }

        if (options.containsKey(EVENT_FIELD_SEEK)) {
            this.eventFieldSeek = Integer.parseInt(options.get(EVENT_FIELD_SEEK));
        }

        if (options.containsKey(EVENT_NEXT_SEEK)) {
            this.eventNextSeek = Integer.parseInt(options.get(EVENT_NEXT_SEEK));
        }

        if (options.containsKey(TF_FIELD_SEEK)) {
            this.tfFieldSeek = Integer.parseInt(options.get(TF_FIELD_SEEK));
        }

        if (options.containsKey(TF_NEXT_SEEK)) {
            this.tfNextSeek = Integer.parseInt(options.get(TF_NEXT_SEEK));
        }

        if (options.containsKey(SEEKING_EVENT_AGGREGATION)) {
            this.seekingEventAggregation = Boolean.parseBoolean(options.get(SEEKING_EVENT_AGGREGATION));
        }

        if (options.containsKey(DOC_AGGREGATION_THRESHOLD_MS)) {
            this.docAggregationThresholdMs = Integer.parseInt(options.get(DOC_AGGREGATION_THRESHOLD_MS));
        }

        if (options.containsKey(TERM_FREQUENCY_AGGREGATION_THRESHOLD_MS)) {
            this.tfAggregationThresholdMs = Integer.parseInt(options.get(TERM_FREQUENCY_AGGREGATION_THRESHOLD_MS));
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

        this.timeFilter = new TimeFilter(startTime, endTime);

        if (options.containsKey(INCLUDE_GROUPING_CONTEXT)) {
            this.setIncludeGroupingContext(Boolean.parseBoolean(options.get(INCLUDE_GROUPING_CONTEXT)));
        }

        if (options.containsKey(DOCUMENT_PERMUTATION_CLASSES)) {
            this.setDocumentPermutationClasses(options.get(DOCUMENT_PERMUTATION_CLASSES));
        }

        if (options.containsKey(LIMIT_FIELDS)) {
            String limitFields = options.get(LIMIT_FIELDS);
            for (String paramGroup : Splitter.on(',').omitEmptyStrings().trimResults().split(limitFields)) {
                String[] keyAndValue = Iterables.toArray(Splitter.on('=').omitEmptyStrings().trimResults().split(paramGroup), String.class);
                if (keyAndValue != null && keyAndValue.length > 1) {
                    this.getLimitFieldsMap().put(keyAndValue[0], Integer.parseInt(keyAndValue[1]));
                }
            }
        }

        if (options.containsKey(MATCHING_FIELD_SETS)) {
            String matchingFieldSets = options.get(MATCHING_FIELD_SETS);
            for (String fieldSet : Splitter.on(',').omitEmptyStrings().trimResults().split(matchingFieldSets)) {
                String[] fields = Iterables.toArray(Splitter.on('=').omitEmptyStrings().trimResults().split(fieldSet), String.class);
                if (fields.length != 0) {
                    this.getMatchingFieldSets().add(new HashSet(Arrays.asList(fields)));
                }
            }
        }

        if (options.containsKey(LIMIT_FIELDS_PRE_QUERY_EVALUATION)) {
            this.setLimitFieldsPreQueryEvaluation(Boolean.parseBoolean(options.get(LIMIT_FIELDS_PRE_QUERY_EVALUATION)));
        }

        if (options.containsKey(LIMIT_FIELDS_FIELD)) {
            this.setLimitFieldsField(options.get(LIMIT_FIELDS_FIELD));
        }

        if (options.containsKey(GROUP_FIELDS)) {
            this.setGroupFields(GroupFields.from(options.get(GROUP_FIELDS)));
        }

        if (options.containsKey(GROUP_FIELDS_BATCH_SIZE)) {
            String groupFieldsBatchSize = options.get(GROUP_FIELDS_BATCH_SIZE);
            int batchSize = Integer.parseInt(groupFieldsBatchSize);
            this.setGroupFieldsBatchSize(batchSize);
        }

        if (options.containsKey(UNIQUE_FIELDS)) {
            this.setUniqueFields(UniqueFields.from(options.get(UNIQUE_FIELDS)));
        }

        if (options.containsKey(HIT_LIST)) {
            log.debug("Adding hitList to QueryOptions? " + options.get(HIT_LIST));
            if (Boolean.parseBoolean(options.get(HIT_LIST))) {
                this.setArithmetic(new HitListArithmetic());
            }
        } else {
            log.debug("options does not include key 'hit.list'");
        }

        if (options.containsKey(DATE_INDEX_TIME_TRAVEL)) {
            log.debug("Adding dateIndexTimeTravel to QueryOptions? " + options.get(DATE_INDEX_TIME_TRAVEL));
            boolean dateIndexTimeTravel = Boolean.parseBoolean(options.get(DATE_INDEX_TIME_TRAVEL));
            if (dateIndexTimeTravel) {
                this.setDateIndexTimeTravel(dateIndexTimeTravel);
            }
        }

        if (options.containsKey(POSTPROCESSING_CLASSES)) {
            this.postProcessingFunctions = options.get(POSTPROCESSING_CLASSES);
            // test parsing of the functions
            getPostProcessingChain(new WrappingIterator<>());
        }

        if (options.containsKey(NON_INDEXED_DATATYPES)) {
            try {

                String nonIndexedDataTypes = options.get(NON_INDEXED_DATATYPES);
                if (compressedMappings) {
                    nonIndexedDataTypes = decompressOption(nonIndexedDataTypes, QueryOptions.UTF8);
                }

                this.setNonIndexedDataTypeMap(buildFieldDataTypeMap(nonIndexedDataTypes));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        if (options.containsKey(CONTAINS_INDEX_ONLY_TERMS)) {
            this.setContainsIndexOnlyTerms(Boolean.parseBoolean(options.get(CONTAINS_INDEX_ONLY_TERMS)));
        }

        if (options.containsKey(ALLOW_FIELD_INDEX_EVALUATION)) {
            this.setAllowFieldIndexEvaluation(Boolean.parseBoolean(options.get(ALLOW_FIELD_INDEX_EVALUATION)));
        }

        if (options.containsKey(ALLOW_TERM_FREQUENCY_LOOKUP)) {
            this.setAllowTermFrequencyLookup(Boolean.parseBoolean(options.get(ALLOW_TERM_FREQUENCY_LOOKUP)));
        }

        if (options.containsKey(HDFS_SITE_CONFIG_URLS)) {
            this.setHdfsSiteConfigURLs(options.get(HDFS_SITE_CONFIG_URLS));
        }

        if (options.containsKey(HDFS_FILE_COMPRESSION_CODEC)) {
            this.setHdfsFileCompressionCodec(options.get(HDFS_FILE_COMPRESSION_CODEC));
        }

        if (options.containsKey(ZOOKEEPER_CONFIG)) {
            this.setZookeeperConfig(options.get(ZOOKEEPER_CONFIG));
        }

        if (options.containsKey(IVARATOR_CACHE_DIR_CONFIG)) {
            try {
                this.setIvaratorCacheDirConfigs(IvaratorCacheDirConfig.fromJson(options.get(IVARATOR_CACHE_DIR_CONFIG)));
            } catch (JsonProcessingException e) {
                log.warn("Unable to parse ivaratorCacheDirConfig.", e);
            }
        }

        if (options.containsKey(IVARATOR_CACHE_BUFFER_SIZE)) {
            this.setIvaratorCacheBufferSize(Integer.parseInt(options.get(IVARATOR_CACHE_BUFFER_SIZE)));
        }

        if (options.containsKey(IVARATOR_SCAN_PERSIST_THRESHOLD)) {
            this.setIvaratorCacheScanPersistThreshold(Long.parseLong(options.get(IVARATOR_SCAN_PERSIST_THRESHOLD)));
        }

        if (options.containsKey(IVARATOR_SCAN_TIMEOUT)) {
            this.setIvaratorCacheScanTimeout(Long.parseLong(options.get(IVARATOR_SCAN_TIMEOUT)));
        }

        if (options.containsKey(RESULT_TIMEOUT)) {
            this.setResultTimeout(Long.parseLong(options.get(RESULT_TIMEOUT)));
        }

        if (options.containsKey(MAX_INDEX_RANGE_SPLIT)) {
            this.setMaxIndexRangeSplit(Integer.parseInt(options.get(MAX_INDEX_RANGE_SPLIT)));
        }

        if (options.containsKey(MAX_IVARATOR_OPEN_FILES)) {
            this.setIvaratorMaxOpenFiles(Integer.parseInt(options.get(MAX_IVARATOR_OPEN_FILES)));
        }

        if (options.containsKey(IVARATOR_NUM_RETRIES)) {
            this.setIvaratorNumRetries(Integer.parseInt(options.get(IVARATOR_NUM_RETRIES)));
        }

        if (options.containsKey(IVARATOR_PERSIST_VERIFY)) {
            boolean verify = Boolean.parseBoolean(options.get(IVARATOR_PERSIST_VERIFY));
            FileSortedSet.PersistOptions persistOptions = getIvaratorPersistOptions();
            this.setIvaratorPersistOptions(new FileSortedSet.PersistOptions(verify, verify, persistOptions.getNumElementsToVerify()));
        }

        if (options.containsKey(IVARATOR_PERSIST_VERIFY_COUNT)) {
            int numElements = Integer.parseInt(options.get(IVARATOR_PERSIST_VERIFY_COUNT));
            FileSortedSet.PersistOptions persistOptions = getIvaratorPersistOptions();
            this.setIvaratorPersistOptions(new FileSortedSet.PersistOptions(persistOptions.isVerifySize(), persistOptions.isVerifyElements(), numElements));
        }

        if (options.containsKey(MAX_IVARATOR_SOURCES)) {
            this.setMaxIvaratorSources(Integer.parseInt(options.get(MAX_IVARATOR_SOURCES)));
        }

        if (options.containsKey(MAX_IVARATOR_SOURCE_WAIT)) {
            this.setMaxIvaratorSourceWait(Long.parseLong(options.get(MAX_IVARATOR_SOURCE_WAIT)));
        }

        if (options.containsKey(MAX_IVARATOR_RESULTS)) {
            this.setMaxIvaratorResults(Long.parseLong(options.get(MAX_IVARATOR_RESULTS)));
        }

        if (options.containsKey(YIELD_THRESHOLD_MS)) {
            this.setYieldThresholdMs(Long.parseLong(options.get(YIELD_THRESHOLD_MS)));
        }

        if (options.containsKey(COMPRESS_SERVER_SIDE_RESULTS)) {
            this.setCompressResults(Boolean.parseBoolean(options.get(COMPRESS_SERVER_SIDE_RESULTS)));
        }

        if (options.containsKey(MAX_EVALUATION_PIPELINES)) {
            this.setMaxEvaluationPipelines(Integer.parseInt(options.get(MAX_EVALUATION_PIPELINES)));
        }

        if (options.containsKey(SERIAL_EVALUATION_PIPELINE)) {
            this.setSerialEvaluationPipeline(Boolean.parseBoolean(options.get(SERIAL_EVALUATION_PIPELINE)));
        }

        if (options.containsKey(MAX_PIPELINE_CACHED_RESULTS)) {
            this.setMaxPipelineCachedResults(Integer.parseInt(options.get(MAX_PIPELINE_CACHED_RESULTS)));
        }

        if (options.containsKey(TERM_FREQUENCIES_REQUIRED)) {
            this.setTermFrequenciesRequired(Boolean.parseBoolean(options.get(TERM_FREQUENCIES_REQUIRED)));
        }
        this.setTermFrequencyFields(parseTermFrequencyFields(options));
        this.setContentExpansionFields(parseContentExpansionFields(options));

        if (options.containsKey(DATE_INDEX_TIME_TRAVEL)) {
            this.dateIndexTimeTravel = Boolean.parseBoolean(options.get(DATE_INDEX_TIME_TRAVEL));
        }

        if (options.containsKey(SORTED_UIDS)) {
            this.sortedUIDs = Boolean.parseBoolean(options.get(SORTED_UIDS));
        }

        if (options.containsKey(DEBUG_MULTITHREADED_SOURCES)) {
            this.debugMultithreadedSources = Boolean.parseBoolean(options.get(DEBUG_MULTITHREADED_SOURCES));
        }

        if (options.containsKey(ACTIVE_QUERY_LOG_NAME)) {
            setActiveQueryLogName(activeQueryLogName);
        }

        if (options.containsKey(EXCERPT_FIELDS)) {
            setExcerptFields(ExcerptFields.from(options.get(EXCERPT_FIELDS)));
        }

        if (options.containsKey(EXCERPT_FIELDS_NO_HIT_CALLOUT)) {
            setExcerptFieldsNoHitCallout(Boolean.parseBoolean(options.get(EXCERPT_FIELDS_NO_HIT_CALLOUT)));
        }

        if (options.containsKey(EXCERPT_ITERATOR)) {
            try {
                setExcerptIterator((Class<? extends SortedKeyValueIterator<Key,Value>>) Class.forName(options.get(EXCERPT_ITERATOR)));
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Could not get class for " + options.get(EXCERPT_ITERATOR), e);
            }
        }

        return true;
    }

    private void setSerialEvaluationPipeline(boolean serialEvaluationPipeline) {
        this.serialEvaluationPipeline = serialEvaluationPipeline;
    }

    protected void validateTypeMetadata(Map<String,String> options) {
        if (options.containsKey(TYPE_METADATA_AUTHS)) {
            String typeMetadataAuthsString = options.get(TYPE_METADATA_AUTHS);
            try {
                if (typeMetadataAuthsString != null && compressedMappings) {
                    typeMetadataAuthsString = decompressOption(typeMetadataAuthsString, QueryOptions.UTF8);
                }
                this.typeMetadataAuthsKey = Sets
                                .newHashSet(Splitter.on(CharMatcher.anyOf(",& ")).omitEmptyStrings().trimResults().split(typeMetadataAuthsString));
            } catch (IOException e) {
                log.warn("could not set typeMetadataAuthsKey from: \"" + typeMetadataAuthsString + "\"");
            }

            if (log.isTraceEnabled()) {
                log.trace("Using typeMetadataAuthsKey: " + this.typeMetadataAuthsKey);
            }
        }
        // Serialized version of a mapping from field name to DataType used
        if (options.containsKey(TYPE_METADATA)) {
            String typeMetadataString = options.get(TYPE_METADATA);
            try {
                if (compressedMappings) {
                    typeMetadataString = decompressOption(typeMetadataString, QueryOptions.UTF8);
                }
                this.typeMetadata = buildTypeMetadata(typeMetadataString);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            if (log.isTraceEnabled()) {
                log.trace("Using typeMetadata: " + this.typeMetadata);
            }
        }
        if (options.containsKey(METADATA_TABLE_NAME)) {
            this.metadataTableName = options.get(METADATA_TABLE_NAME);
        }

    }

    protected static String decompressOption(final String buffer, Charset characterSet) throws IOException {
        final byte[] inBase64 = Base64.decodeBase64(buffer.getBytes());

        ByteArrayInputStream byteInputStream = new ByteArrayInputStream(inBase64);

        GZIPInputStream gzipInputStream = new GZIPInputStream(byteInputStream);

        DataInputStream dataInputStream = new DataInputStream(gzipInputStream);

        final int length = dataInputStream.readInt();
        final byte[] dataBytes = new byte[length];
        dataInputStream.readFully(dataBytes, 0, length);

        dataInputStream.close();
        gzipInputStream.close();

        return new String(dataBytes, characterSet);
    }

    /**
     * Restore the mapping of field name to dataTypes from a String-ified representation
     *
     * @param data
     *            the data
     * @return a mapping of field name to data types
     */
    public static Map<String,Set<String>> buildFieldDataTypeMap(String data) {

        Map<String,Set<String>> mapping = new HashMap<>();

        if (org.apache.commons.lang3.StringUtils.isNotBlank(data)) {
            String[] entries = StringUtils.split(data, ';');
            for (String entry : entries) {
                String[] entrySplits = StringUtils.split(entry, ':');

                if (2 != entrySplits.length) {
                    log.warn("Skipping unparseable normalizer entry: '" + entry + "', from '" + data + "'");
                } else {
                    String[] values = StringUtils.split(entrySplits[1], ',');
                    HashSet<String> dataTypes = new HashSet<>();

                    Collections.addAll(dataTypes, values);

                    mapping.put(entrySplits[0], dataTypes);

                    if (log.isTraceEnabled()) {
                        log.trace("Adding " + entrySplits[0] + " " + dataTypes);
                    }
                }
            }
        }

        return mapping;
    }

    public static Set<String> fetchDataTypeKeys(String data) {
        Set<String> keys = Sets.newHashSet();
        if (org.apache.commons.lang3.StringUtils.isNotBlank(data)) {
            String[] entries = StringUtils.split(data, ';');
            for (String entry : entries) {
                String[] entrySplits = StringUtils.split(entry, ':');

                if (2 != entrySplits.length) {
                    log.warn("Skipping unparseable normalizer entry: '" + entry + "', from '" + data + "'");
                } else {
                    keys.add(entrySplits[0]);

                    if (log.isTraceEnabled()) {
                        log.trace("Adding " + entrySplits[0] + " " + keys);
                    }
                }
            }
        }

        return keys;
    }

    public static TypeMetadata buildTypeMetadata(String data) {
        return new TypeMetadata(data);
    }

    /**
     * Build a String-ified version of the Map to serialize to this SKVI.
     *
     * @param map
     *            a map to normalize
     * @return the string representation of the map
     */
    public static String buildFieldNormalizerString(Map<String,Set<String>> map) {
        StringBuilder sb = new StringBuilder();

        for (Entry<String,Set<String>> entry : map.entrySet()) {
            if (sb.length() > 0) {
                sb.append(';');
            }

            sb.append(entry.getKey()).append(':');

            boolean first = true;
            for (String val : entry.getValue()) {
                if (!first) {
                    sb.append(',');
                }

                sb.append(val);
                first = false;
            }
        }

        return sb.toString();
    }

    /**
     * Build a String-ified version of the Map to serialize to this SKVI.
     *
     * @param map
     *            a map to normalize
     * @return the string representation of the map
     */
    public static String buildFieldNormalizerString(Multimap<String,Type<?>> map) {
        StringBuilder sb = new StringBuilder();

        for (String fieldName : map.keySet()) {
            if (sb.length() > 0) {
                sb.append(';');
            }

            sb.append(fieldName).append(':');

            boolean first = true;
            for (Type<?> type : map.get(fieldName)) {
                if (!first) {
                    sb.append(',');
                }

                sb.append(type.getClass().getName());
                first = false;
            }
        }

        return sb.toString();
    }

    public static String compressOption(final String data, final Charset characterSet) throws IOException {
        final ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        final GZIPOutputStream gzipStream = new GZIPOutputStream(byteStream);
        final DataOutputStream dataOut = new DataOutputStream(gzipStream);

        byte[] arr = data.getBytes(characterSet);
        final int length = arr.length;

        dataOut.writeInt(length);
        dataOut.write(arr);

        dataOut.close();
        byteStream.close();

        return new String(Base64.encodeBase64(byteStream.toByteArray()));
    }

    public static String buildFieldStringFromSet(Collection<String> fields) {
        StringBuilder sb = new StringBuilder();
        for (String field : fields) {
            if (sb.length() > 0) {
                sb.append(',');
            }

            sb.append(field);
        }

        return sb.toString();
    }

    public static Set<String> buildFieldSetFromString(String fieldStr) {
        Set<String> fields = new HashSet<>();
        for (String field : StringUtils.split(fieldStr, ',')) {
            if (!org.apache.commons.lang.StringUtils.isBlank(field)) {
                fields.add(field);
            }
        }
        return fields;
    }

    public static String buildIgnoredColumnFamiliesString(Collection<String> colFams) {
        StringBuilder sb = new StringBuilder();
        for (String cf : colFams) {
            if (sb.length() > 0) {
                sb.append(',');
            }

            sb.append(cf);
        }

        return sb.toString();
    }

    /**
     * Get a serialization and deserialization utility for {@link datawave.query.util.count.CountMap}
     *
     * @return count map utility
     */
    private CountMapSerDe getMapSerDe() {
        if (mapSerDe == null) {
            mapSerDe = new CountMapSerDe();
        }
        return mapSerDe;
    }

    public static Set<String> buildIgnoredColumnFamilies(String colFams) {
        return Sets.newHashSet(StringUtils.split(colFams, ','));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public Iterator<Entry<Key,Document>> getPostProcessingChain(Iterator<Entry<Key,Document>> postProcessingBase) {
        String functions = postProcessingFunctions;
        if (functions != null && !functions.isEmpty()) {
            try {
                Iterator tforms = postProcessingBase;
                for (String fClassName : StringUtils.splitIterable(functions, ',', true)) {
                    if (log.isTraceEnabled()) {
                        log.trace("Configuring post-processing class: " + fClassName);
                    }
                    Class<?> fClass = Class.forName(fClassName);
                    if (Function.class.isAssignableFrom(fClass)) {
                        Function f = (Function) fClass.getDeclaredConstructor().newInstance();

                        if (f instanceof ConfiguredFunction) {
                            ((ConfiguredFunction) f).configure(options);
                        }

                        tforms = Iterators.transform(tforms, f);
                    } else if (Predicate.class.isAssignableFrom(fClass)) {
                        Predicate p = (Predicate) fClass.getDeclaredConstructor().newInstance();

                        if (p instanceof ConfiguredPredicate) {
                            ((ConfiguredPredicate) p).configure(options);
                        }

                        tforms = QueryIterator.statelessFilter(tforms, p);
                    } else {
                        log.error(fClass + " is not a function or predicate.");
                        throw new RuntimeException(fClass + " is not a function or predicate.");
                    }
                }
                return tforms;
            } catch (ClassNotFoundException | IllegalAccessException | InstantiationException | NoSuchMethodException | InvocationTargetException e) {
                log.error("Could not instantiate postprocessing chain!", e);
                throw new RuntimeException("Could not instantiate postprocessing chain!", e);
            }
        }
        return postProcessingBase;
    }

    public boolean isTermFrequenciesRequired() {
        return termFrequenciesRequired;
    }

    public void setTermFrequenciesRequired(boolean termFrequenciesRequired) {
        this.termFrequenciesRequired = termFrequenciesRequired;
    }

    public Set<String> parseTermFrequencyFields(Map<String,String> options) {
        String val = options.get(TERM_FREQUENCY_FIELDS);
        if (val == null) {
            return Collections.emptySet();
        } else {
            return ImmutableSet.copyOf(Splitter.on(',').trimResults().split(val));
        }
    }

    public Set<String> getTermFrequencyFields() {
        return termFrequencyFields;
    }

    public void setTermFrequencyFields(Set<String> termFrequencyFields) {
        this.termFrequencyFields = termFrequencyFields;
    }

    public Set<String> parseContentExpansionFields(Map<String,String> options) {
        String val = options.get(CONTENT_EXPANSION_FIELDS);
        if (val == null) {
            return Collections.emptySet();
        } else {
            return ImmutableSet.copyOf(Splitter.on(',').trimResults().split(val));
        }
    }

    public Set<String> getContentExpansionFields() {
        return contentExpansionFields;
    }

    public void setContentExpansionFields(Set<String> contentExpansionFields) {
        this.contentExpansionFields = contentExpansionFields;
    }

    public int getMaxEvaluationPipelines() {
        return maxEvaluationPipelines;
    }

    public void setMaxEvaluationPipelines(int maxEvaluationPipelines) {
        this.maxEvaluationPipelines = maxEvaluationPipelines;
    }

    public int getMaxPipelineCachedResults() {
        return maxPipelineCachedResults;
    }

    public void setMaxPipelineCachedResults(int maxCachedResults) {
        this.maxPipelineCachedResults = maxCachedResults;
    }

    public String getStatsdHostAndPort() {
        return statsdHostAndPort;
    }

    public void setStatsdHostAndPort(String statsdHostAndPort) {
        this.statsdHostAndPort = statsdHostAndPort;
    }

    public QueryStatsDClient getStatsdClient() {
        if (statsdHostAndPort != null && queryId != null) {
            if (statsdClient == null) {
                synchronized (queryId) {
                    if (statsdClient == null) {
                        setStatsdClient(new QueryStatsDClient(queryId, getStatsdHost(statsdHostAndPort), getStatsdPort(statsdHostAndPort),
                                        getStatsdMaxQueueSize()));
                    }
                }
            }
        }
        return statsdClient;
    }

    private String getStatsdHost(String statsdHostAndPort) {
        int index = statsdHostAndPort.indexOf(':');
        if (index == -1) {
            return statsdHostAndPort;
        } else if (index == 0) {
            return "localhost";
        } else {
            return statsdHostAndPort.substring(0, index);
        }
    }

    private int getStatsdPort(String statsdHostAndPort) {
        int index = statsdHostAndPort.indexOf(':');
        if (index == -1) {
            return 8125;
        } else if (index == statsdHostAndPort.length() - 1) {
            return 8125;
        } else {
            return Integer.parseInt(statsdHostAndPort.substring(index + 1));
        }
    }

    public void setStatsdClient(QueryStatsDClient statsdClient) {
        this.statsdClient = statsdClient;
    }

    public int getStatsdMaxQueueSize() {
        return statsdMaxQueueSize;
    }

    public void setStatsdMaxQueueSize(int statsdMaxQueueSize) {
        this.statsdMaxQueueSize = statsdMaxQueueSize;
    }

    public long getYieldThresholdMs() {
        return yieldThresholdMs;
    }

    public void setYieldThresholdMs(long yieldThresholdMs) {
        this.yieldThresholdMs = yieldThresholdMs;
    }

    public int getFiFieldSeek() {
        return fiFieldSeek;
    }

    public void setFiFieldSeek(int fiFieldSeek) {
        this.fiFieldSeek = fiFieldSeek;
    }

    public int getFiNextSeek() {
        return fiNextSeek;
    }

    public void setFiNextSeek(int fiNextSeek) {
        this.fiNextSeek = fiNextSeek;
    }

    public int getEventFieldSeek() {
        return eventFieldSeek;
    }

    public void setEventFieldSeek(int eventFieldSeek) {
        this.eventFieldSeek = eventFieldSeek;
    }

    public int getEventNextSeek() {
        return eventNextSeek;
    }

    public void setEventNextSeek(int eventNextSeek) {
        this.eventNextSeek = eventNextSeek;
    }

    public int getTfFieldSeek() {
        return tfFieldSeek;
    }

    public void setTfFieldSeek(int tfFieldSeek) {
        this.tfFieldSeek = tfFieldSeek;
    }

    public int getTfNextSeek() {
        return tfNextSeek;
    }

    public void setTfNextSeek(int tfNextSeek) {
        this.tfNextSeek = tfNextSeek;
    }

    public int getDocAggregationThresholdMs() {
        return docAggregationThresholdMs;
    }

    public void setDocAggregationThresholdMs(int docAggregationThresholdMs) {
        this.docAggregationThresholdMs = docAggregationThresholdMs;
    }

    public int getTfAggregationThresholdMs() {
        return tfAggregationThresholdMs;
    }

    public void setTfAggregationThresholdMs(int tfAggregationThresholdMs) {
        this.tfAggregationThresholdMs = tfAggregationThresholdMs;
    }

    /**
     * Get an {@link Equality}
     *
     * @return an Equality
     */
    public Equality getEquality() {
        if (equality == null) {
            equality = new PrefixEquality(PartialKey.ROW_COLFAM);
        }
        return equality;
    }

    public boolean isSeekingEventAggregation() {
        return seekingEventAggregation;
    }
}
