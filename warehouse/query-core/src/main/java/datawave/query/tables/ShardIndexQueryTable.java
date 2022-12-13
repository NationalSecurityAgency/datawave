package datawave.query.tables;

import com.google.common.base.Function;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import datawave.data.type.Type;
import datawave.query.QueryParameters;
import datawave.query.config.ShardIndexQueryConfiguration;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.discovery.DiscoveredThing;
import datawave.query.discovery.DiscoveryIterator;
import datawave.query.discovery.DiscoveryTransformer;
import datawave.query.exceptions.EmptyUnfieldedTermExpansionException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.lookups.ShardIndexQueryTableStaticMethods;
import datawave.query.jexl.visitors.ExpandMultiNormalizedTerms;
import datawave.query.jexl.visitors.FetchDataTypesVisitor;
import datawave.query.jexl.visitors.LiteralNodeVisitor;
import datawave.query.jexl.visitors.PatternNodeVisitor;
import datawave.query.jexl.visitors.QueryModelVisitor;
import datawave.query.jexl.visitors.UnfieldedIndexExpansionVisitor;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import datawave.query.language.tree.QueryNode;
import datawave.query.model.QueryModel;
import datawave.query.util.MetadataHelper;
import datawave.query.util.MetadataHelperFactory;
import datawave.tables.TableName;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.logic.BaseQueryLogic;
import datawave.webservice.query.logic.QueryLogicTransformer;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.LongRange;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Iterators.concat;
import static com.google.common.collect.Iterators.transform;
import static datawave.query.config.ShardQueryConfiguration.PARAM_VALUE_SEP_STR;

/**
 * Query Table implementation that accepts a single term and returns information from the global index for that term. The response includes the number of
 * occurrences of the term by type by day.
 */
public class ShardIndexQueryTable extends BaseQueryLogic<DiscoveredThing> {
    
    private static final Logger log = Logger.getLogger(ShardIndexQueryTable.class);
    private String indexTableName;
    private String reverseIndexTableName;
    private boolean fullTableScanEnabled = true;
    private boolean allowLeadingWildcard = true;
    private List<String> realmSuffixExclusionPatterns = null;
    protected String modelName = "DATAWAVE";
    protected String modelTableName = "DatawaveMetadata";
    protected MetadataHelperFactory metadataHelperFactory;
    protected ScannerFactory scannerFactory;
    protected QueryModel queryModel;
    
    public ShardIndexQueryTable() {}
    
    public ShardIndexQueryTable(ShardIndexQueryTable other) {
        super(other);
        this.indexTableName = other.getIndexTableName();
        this.reverseIndexTableName = other.getReverseIndexTableName();
        this.fullTableScanEnabled = other.isFullTableScanEnabled();
        this.allowLeadingWildcard = other.isAllowLeadingWildcard();
        this.queryModel = other.getQueryModel();
        this.modelName = other.getModelName();
        this.modelTableName = other.getModelTableName();
        this.metadataHelperFactory = other.getMetadataHelperFactory();
        this.setRealmSuffixExclusionPatterns(other.getRealmSuffixExclusionPatterns());
    }
    
    @Override
    public ShardIndexQueryTable clone() {
        return new ShardIndexQueryTable(this);
    }
    
    @Override
    public void close() {
        super.close();
        
        if (scannerFactory == null) {
            if (log.isDebugEnabled()) {
                log.debug("ScannerFactory is null; not closing it.");
            }
        } else {
            int nClosed = 0;
            scannerFactory.lockdown();
            for (ScannerBase bs : scannerFactory.currentScanners()) {
                scannerFactory.close(bs);
                ++nClosed;
            }
            if (log.isDebugEnabled()) {
                log.debug("Cleaned up " + nClosed + " batch scanners associated with this query logic.");
            }
        }
    }
    
    /**
     * Create and initialize a metadata helper
     * 
     * @param connector
     *            the connector
     * @param metadataTableName
     *            metadata table name
     * @param auths
     *            a set of auths
     * @return a new initialized MetadataHelper
     */
    protected MetadataHelper initializeMetadataHelper(Connector connector, String metadataTableName, Set<Authorizations> auths) {
        return this.metadataHelperFactory.createMetadataHelper(connector, metadataTableName, auths);
    }
    
    public MetadataHelperFactory getMetadataHelperFactory() {
        return metadataHelperFactory;
    }
    
    public void setMetadataHelperFactory(MetadataHelperFactory metadataHelperFactory) {
        this.metadataHelperFactory = metadataHelperFactory;
    }
    
    public String getIndexTableName() {
        return indexTableName;
    }
    
    public void setIndexTableName(String indexTableName) {
        this.indexTableName = indexTableName;
    }
    
    public String getReverseIndexTableName() {
        return reverseIndexTableName;
    }
    
    public void setReverseIndexTableName(String reverseIndexTableName) {
        this.reverseIndexTableName = reverseIndexTableName;
    }
    
    @Override
    public GenericQueryConfiguration initialize(Connector connection, Query settings, Set<Authorizations> auths) throws Exception {
        ShardIndexQueryConfiguration config = new ShardIndexQueryConfiguration(this, settings);
        this.scannerFactory = new ScannerFactory(connection);
        MetadataHelper metadataHelper = initializeMetadataHelper(connection, config.getMetadataTableName(), auths);
        
        if (StringUtils.isEmpty(settings.getQuery())) {
            throw new IllegalArgumentException("Query cannot be null");
        }
        
        if (log.isDebugEnabled()) {
            log.debug("Query parameters set to " + settings.getParameters());
        }
        
        String tModelName = getTrimmedOrNull(settings, QueryParameters.PARAMETER_MODEL_NAME);
        if (tModelName != null) {
            modelName = tModelName;
        }
        
        String tModelTableName = getTrimmedOrNull(settings, QueryParameters.PARAMETER_MODEL_TABLE_NAME);
        if (tModelTableName != null) {
            modelTableName = tModelTableName;
        }
        
        queryModel = metadataHelper.getQueryModel(modelTableName, modelName, null);
        
        String datatypeFilterString = getTrimmedOrNull(settings, QueryParameters.DATATYPE_FILTER_SET);
        if (datatypeFilterString != null) {
            config.setDatatypeFilter(new HashSet<>(Arrays.asList(datatypeFilterString.split(PARAM_VALUE_SEP_STR))));
            if (log.isDebugEnabled()) {
                log.debug("Data type filter set to " + config.getDatatypeFilterAsString());
            }
        }
        
        config.setConnector(connection);
        config.setAuthorizations(auths);
        
        if (indexTableName != null) {
            config.setIndexTableName(indexTableName);
        }
        
        if (reverseIndexTableName != null) {
            config.setReverseIndexTableName(reverseIndexTableName);
        }
        
        if (settings.getBeginDate() != null) {
            config.setBeginDate(settings.getBeginDate());
        } else {
            config.setBeginDate(new Date(0));
            if (log.isDebugEnabled()) {
                log.debug("No begin date supplied in settings.");
            }
        }
        
        if (settings.getEndDate() != null) {
            config.setEndDate(settings.getEndDate());
        } else {
            config.setEndDate(new Date(Long.MAX_VALUE));
            if (log.isDebugEnabled()) {
                log.debug("No end date supplied in settings.");
            }
        }
        
        // start with a trimmed version of the query, converted to JEXL
        LuceneToJexlQueryParser parser = new LuceneToJexlQueryParser();
        parser.setAllowLeadingWildCard(this.isAllowLeadingWildcard());
        QueryNode node = parser.parse(settings.getQuery().trim());
        // TODO: Validate that this is a simple list of terms type of query
        config.setQueryString(node.getOriginalQuery());
        if (log.isDebugEnabled()) {
            log.debug("Original Query = " + settings.getQuery().trim());
            log.debug("JEXL Query = " + node.getOriginalQuery());
        }
        
        // Parse & flatten the query.
        ASTJexlScript origScript = JexlASTHelper.parseAndFlattenJexlQuery(config.getQueryString());
        
        ASTJexlScript script;
        try {
            script = UnfieldedIndexExpansionVisitor.expandUnfielded(config, this.scannerFactory, metadataHelper, origScript);
        } catch (EmptyUnfieldedTermExpansionException e) {
            Multimap<String,String> emptyMap = Multimaps.unmodifiableMultimap(HashMultimap.create());
            config.setNormalizedTerms(emptyMap);
            config.setNormalizedPatterns(emptyMap);
            return config;
        }
        
        Set<String> dataTypes = config.getDatatypeFilter();
        Set<String> allFields = metadataHelper.getAllFields(dataTypes);
        
        script = QueryModelVisitor.applyModel(script, queryModel, allFields);
        
        if (log.isTraceEnabled()) {
            log.trace("fetching dataTypes from FetchDataTypesVisitor");
        }
        Multimap<String,Type<?>> fieldToDataTypeMap = FetchDataTypesVisitor.fetchDataTypes(metadataHelper, config.getDatatypeFilter(), script);
        config.setDataTypes(fieldToDataTypeMap);
        config.setQueryFieldsDatatypes(fieldToDataTypeMap);
        
        final Set<String> indexedFields = metadataHelper.getIndexedFields(dataTypes);
        config.setIndexedFields(indexedFields);
        
        final Set<String> reverseIndexedFields = metadataHelper.getReverseIndexedFields(dataTypes);
        config.setReverseIndexedFields(reverseIndexedFields);
        
        final Multimap<String,Type<?>> normalizedFields = metadataHelper.getFieldsToDatatypes(dataTypes);
        config.setNormalizedFieldsDatatypes(normalizedFields);
        
        if (log.isTraceEnabled()) {
            log.trace("Normalizers:");
            for (String field : fieldToDataTypeMap.keySet()) {
                log.trace(field + ": " + fieldToDataTypeMap.get(field));
            }
        }
        
        script = ExpandMultiNormalizedTerms.expandTerms(config, metadataHelper, script);
        
        Multimap<String,String> literals = LiteralNodeVisitor.getLiterals(script);
        Multimap<String,String> patterns = PatternNodeVisitor.getPatterns(script);
        Map<Entry<String,String>,Range> rangesForTerms = Maps.newHashMap();
        Map<Entry<String,String>,Entry<Range,Boolean>> rangesForPatterns = Maps.newHashMap();
        
        config.setNormalizedTerms(literals);
        config.setNormalizedPatterns(patterns);
        
        if (log.isDebugEnabled()) {
            log.debug("Normalized Literals = " + literals);
            log.debug("Normalized Patterns = " + patterns);
        }
        
        for (Entry<String,String> entry : literals.entries()) {
            rangesForTerms.put(entry, ShardIndexQueryTableStaticMethods.getLiteralRange(entry));
            
        }
        for (Entry<String,String> entry : patterns.entries()) {
            ShardIndexQueryTableStaticMethods.RefactoredRangeDescription r = ShardIndexQueryTableStaticMethods.getRegexRange(entry, isFullTableScanEnabled(),
                            metadataHelper, config);
            
            rangesForPatterns.put(entry, Maps.immutableEntry(r.range, r.isForReverseIndex));
        }
        
        config.setRangesForTerms(rangesForTerms);
        config.setRangesForPatterns(rangesForPatterns);
        
        return config;
    }
    
    private String getTrimmedOrNull(Query settings, String value) {
        QueryImpl.Parameter param = settings.findParameter(value);
        if (param == null) {
            return null;
        }
        
        String val = param.getParameterValue().trim();
        if (val.isEmpty()) {
            return null;
        }
        
        return val;
    }
    
    @Override
    public void setupQuery(GenericQueryConfiguration genericConfig) throws QueryException, TableNotFoundException, IOException, ExecutionException {
        if (!genericConfig.getClass().getName().equals(ShardIndexQueryConfiguration.class.getName())) {
            throw new QueryException("Did not receive a ShardIndexQueryConfiguration instance!!");
        }
        
        ShardIndexQueryConfiguration config = (ShardIndexQueryConfiguration) genericConfig;
        final List<Entry<BatchScanner,Boolean>> batchscanners = Lists.newLinkedList();
        
        for (Entry<String,String> termEntry : config.getNormalizedTerms().entries()) {
            // scan the table
            BatchScanner bs = configureBatchScannerForDiscovery(config, this.scannerFactory, TableName.SHARD_INDEX,
                            Collections.singleton(config.getRangesForTerms().get(termEntry)), Collections.singleton(termEntry.getValue()),
                            Collections.emptySet(), config.getTableName().equals(config.getReverseIndexTableName()), false,
                            Collections.singleton(termEntry.getKey()));
            
            batchscanners.add(Maps.immutableEntry(bs, false));
        }
        
        for (Entry<String,String> patternEntry : config.getNormalizedPatterns().entries()) {
            Entry<Range,Boolean> rangeEntry = config.getRangesForPatterns().get(patternEntry);
            String tName = rangeEntry.getValue() ? TableName.SHARD_RINDEX : TableName.SHARD_INDEX;
            
            // scan the table
            BatchScanner bs = configureBatchScannerForDiscovery(config, this.scannerFactory, tName, Collections.singleton(rangeEntry.getKey()),
                            Collections.emptySet(), Collections.singleton(patternEntry.getValue()), rangeEntry.getValue(), false,
                            Collections.singleton(patternEntry.getKey()));
            
            batchscanners.add(Maps.immutableEntry(bs, rangeEntry.getValue()));
        }
        
        final Iterator<Entry<BatchScanner,Boolean>> batchScannerIterator = batchscanners.iterator();
        
        this.iterator = concat(transform(new CloseableIterator(batchScannerIterator), new Function<Entry<Key,Value>,Iterator<DiscoveredThing>>() {
            DataInputBuffer in = new DataInputBuffer();
            
            @Override
            public Iterator<DiscoveredThing> apply(Entry<Key,Value> from) {
                Value value = from.getValue();
                in.reset(value.get(), value.getSize());
                ArrayWritable aw = new ArrayWritable(DiscoveredThing.class);
                try {
                    aw.readFields(in);
                } catch (IOException e) {
                    return null;
                }
                ArrayList<DiscoveredThing> thangs = Lists.newArrayListWithCapacity(aw.get().length);
                for (Writable w : aw.get()) {
                    thangs.add((DiscoveredThing) w);
                }
                return thangs.iterator();
            }
        }));
        
        this.scanner = new ScannerBase() {
            
            @Override
            public void addScanIterator(IteratorSetting cfg) {}
            
            @Override
            public void clearColumns() {}
            
            @Override
            public void clearScanIterators() {}
            
            @Override
            public void close() {}
            
            @Override
            public Authorizations getAuthorizations() {
                return null;
            }
            
            @Override
            public void setSamplerConfiguration(SamplerConfiguration samplerConfiguration) {
                
            }
            
            @Override
            public SamplerConfiguration getSamplerConfiguration() {
                return null;
            }
            
            @Override
            public void clearSamplerConfiguration() {
                
            }
            
            @Override
            public void setBatchTimeout(long l, TimeUnit timeUnit) {
                
            }
            
            @Override
            public long getBatchTimeout(TimeUnit timeUnit) {
                return 0;
            }
            
            @Override
            public void setClassLoaderContext(String s) {
                
            }
            
            @Override
            public void clearClassLoaderContext() {
                
            }
            
            @Override
            public String getClassLoaderContext() {
                return null;
            }
            
            @Override
            public void fetchColumn(Text colFam, Text colQual) {}
            
            @Override
            public void fetchColumn(IteratorSetting.Column column) {
                
            }
            
            @Override
            public void fetchColumnFamily(Text col) {}
            
            @Override
            public long getTimeout(TimeUnit timeUnit) {
                return 0;
            }
            
            @Override
            public Iterator<Entry<Key,Value>> iterator() {
                return null;
            }
            
            @Override
            public void removeScanIterator(String iteratorName) {}
            
            @Override
            public void setTimeout(long timeOut, TimeUnit timeUnit) {}
            
            @Override
            public void updateScanIteratorOption(String iteratorName, String key, String value) {}
            
        };
    }
    
    /**
     * scan a global index (shardIndex or shardReverseIndex) for the specified ranges and create a set of fieldname/TermInformation values. The Key/Values
     * scanned are trimmed based on a set of terms to match, and a set of data types (found in the config)
     *
     * @param config
     *            the shard config
     * @param scannerFactory
     *            the scanner factory
     * @param tableName
     *            the table name
     * @param ranges
     *            a set of ranges
     * @param literals
     *            the list of literals
     * @param patterns
     *            the list of patterns
     * @param reverseIndex
     *            the reverse index flag
     * @param expansionFields
     *            the expansion fields
     * @return the batch scanner
     * @throws TableNotFoundException
     *             if the table is not found
     */
    public static BatchScanner configureBatchScanner(ShardQueryConfiguration config, ScannerFactory scannerFactory, String tableName, Collection<Range> ranges,
                    Collection<String> literals, Collection<String> patterns, boolean reverseIndex, Collection<String> expansionFields)
                    throws TableNotFoundException {
        
        // if we have no ranges, then nothing to scan
        if (ranges.isEmpty()) {
            return null;
        }
        
        if (log.isTraceEnabled()) {
            log.trace("Scanning " + tableName + " against " + ranges + " with auths " + config.getAuthorizations());
        }
        
        BatchScanner bs = scannerFactory.newScanner(tableName, config.getAuthorizations(), config.getNumQueryThreads(), config.getQuery());
        
        bs.setRanges(ranges);
        
        // The begin date from the query may be down to the second, for doing lookups in the index we want to use the day because
        // the times in the index table have been truncated to the day.
        Date begin = DateUtils.truncate(config.getBeginDate(), Calendar.DAY_OF_MONTH);
        // we don't need to bump up the end date any more because it's not apart of the range set on the scanner
        Date end = config.getEndDate();
        
        LongRange dateRange = new LongRange(begin.getTime(), end.getTime());
        
        ShardIndexQueryTableStaticMethods.configureGlobalIndexDateRangeFilter(config, bs, dateRange);
        ShardIndexQueryTableStaticMethods.configureGlobalIndexDataTypeFilter(config, bs, config.getDatatypeFilter());
        
        ShardIndexQueryTableStaticMethods.configureGlobalIndexTermMatchingIterator(config, bs, literals, patterns, reverseIndex, true, expansionFields);
        
        return bs;
    }
    
    public static BatchScanner configureBatchScannerForDiscovery(ShardQueryConfiguration config, ScannerFactory scannerFactory, String tableName,
                    Collection<Range> ranges, Collection<String> literals, Collection<String> patterns, boolean reverseIndex, boolean uniqueTermsOnly,
                    Collection<String> expansionFields) throws TableNotFoundException {
        
        // if we have no ranges, then nothing to scan
        if (ranges.isEmpty()) {
            return null;
        }
        
        BatchScanner bs = scannerFactory.newScanner(tableName, config.getAuthorizations(), config.getNumQueryThreads(), config.getQuery());
        
        bs.setRanges(ranges);
        
        // The begin date from the query may be down to the second, for doing lookups in the index we want to use the day because
        // the times in the index table have been truncated to the day.
        Date begin = DateUtils.truncate(config.getBeginDate(), Calendar.DAY_OF_MONTH);
        // we don't need to bump up the end date any more because it's not apart of the range set on the scanner
        Date end = config.getEndDate();
        
        LongRange dateRange = new LongRange(begin.getTime(), end.getTime());
        
        ShardIndexQueryTableStaticMethods.configureGlobalIndexDateRangeFilter(config, bs, dateRange);
        ShardIndexQueryTableStaticMethods.configureGlobalIndexDataTypeFilter(config, bs, config.getDatatypeFilter());
        
        ShardIndexQueryTableStaticMethods.configureGlobalIndexTermMatchingIterator(config, bs, literals, patterns, reverseIndex, uniqueTermsOnly,
                        expansionFields);
        
        bs.addScanIterator(new IteratorSetting(config.getBaseIteratorPriority() + 50, DiscoveryIterator.class));
        
        return bs;
    }
    
    public boolean isFullTableScanEnabled() {
        return fullTableScanEnabled;
    }
    
    public void setFullTableScanEnabled(boolean fullTableScanEnabled) {
        this.fullTableScanEnabled = fullTableScanEnabled;
    }
    
    public boolean isAllowLeadingWildcard() {
        return allowLeadingWildcard;
    }
    
    public void setAllowLeadingWildcard(boolean allowLeadingWildcard) {
        this.allowLeadingWildcard = allowLeadingWildcard;
    }
    
    @Override
    public AccumuloConnectionFactory.Priority getConnectionPriority() {
        return AccumuloConnectionFactory.Priority.NORMAL;
    }
    
    @Override
    public QueryLogicTransformer getTransformer(Query settings) {
        return new DiscoveryTransformer(this, settings, this.queryModel);
    }
    
    /**
     * Query model
     * 
     * @return the model name
     */
    public String getModelName() {
        return modelName;
    }
    
    public void setModelName(String modelName) {
        this.modelName = modelName;
    }
    
    public String getModelTableName() {
        return modelTableName;
    }
    
    public void setModelTableName(String modelTableName) {
        this.modelTableName = modelTableName;
    }
    
    public QueryModel getQueryModel() {
        return this.queryModel;
    }
    
    public void setQueryModel(QueryModel model) {
        this.queryModel = model;
    }
    
    public class CloseableIterator implements Iterator<Entry<Key,Value>> {
        
        private final Iterator<Entry<BatchScanner,Boolean>> batchScannerIterator;
        
        protected Boolean reverseIndex = false;
        protected Entry<Key,Value> currentEntry = null;
        protected BatchScanner currentBS = null;
        protected Iterator<Entry<Key,Value>> currentIter = null;
        
        protected volatile boolean closed = false;
        
        public CloseableIterator(Iterator<Entry<BatchScanner,Boolean>> batchScannerIterator) {
            this.batchScannerIterator = batchScannerIterator;
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see java.util.Iterator#hasNext()
         */
        @Override
        public boolean hasNext() {
            if (closed) {
                return false;
            }
            
            if (null != this.currentEntry) {
                return true;
            } else if (null != this.currentBS && null != this.currentIter) {
                if (this.currentIter.hasNext()) {
                    this.currentEntry = this.currentIter.next();
                    
                    return hasNext();
                } else {
                    this.currentBS.close();
                }
            }
            
            if (batchScannerIterator.hasNext()) {
                Entry<BatchScanner,Boolean> entry = batchScannerIterator.next();
                this.currentBS = entry.getKey();
                this.reverseIndex = entry.getValue();
                this.currentIter = this.currentBS.iterator();
                
                return hasNext();
            }
            
            return false;
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see java.util.Iterator#next()
         */
        @Override
        public Entry<Key,Value> next() {
            if (closed) {
                return null;
            }
            
            if (hasNext()) {
                Entry<Key,Value> cur = this.currentEntry;
                this.currentEntry = null;
                
                if (this.reverseIndex) {
                    Text term = new Text((new StringBuilder(cur.getKey().getRow().toString())).reverse().toString());
                    cur = Maps.immutableEntry(new Key(term, cur.getKey().getColumnFamily(), cur.getKey().getColumnQualifier(), cur.getKey()
                                    .getColumnVisibility(), cur.getKey().getTimestamp()), cur.getValue());
                }
                
                return cur;
            }
            
            return null;
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see java.util.Iterator#remove()
         */
        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
        
        public void close() {
            if (!closed) {
                closed = true;
            }
            
            if (null != this.currentBS) {
                this.currentBS.close();
            }
        }
    }
    
    @Override
    public Set<String> getOptionalQueryParameters() {
        Set<String> optionalParams = new TreeSet<>();
        optionalParams.add(QueryParameters.PARAMETER_MODEL_NAME);
        optionalParams.add(QueryParameters.PARAMETER_MODEL_TABLE_NAME);
        optionalParams.add(QueryParameters.DATATYPE_FILTER_SET);
        optionalParams.add(datawave.webservice.query.QueryParameters.QUERY_BEGIN);
        optionalParams.add(datawave.webservice.query.QueryParameters.QUERY_END);
        return optionalParams;
    }
    
    @Override
    public Set<String> getRequiredQueryParameters() {
        return Collections.emptySet();
    }
    
    @Override
    public Set<String> getExampleQueries() {
        return Collections.emptySet();
    }
    
    public List<String> getRealmSuffixExclusionPatterns() {
        return realmSuffixExclusionPatterns;
    }
    
    public void setRealmSuffixExclusionPatterns(List<String> realmSuffixExclusionPatterns) {
        this.realmSuffixExclusionPatterns = realmSuffixExclusionPatterns;
    }
    
}
