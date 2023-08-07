package datawave.query.tables;

import static com.google.common.collect.Iterators.concat;
import static com.google.common.collect.Iterators.transform;
import static datawave.query.config.ShardQueryConfiguration.PARAM_VALUE_SEP_STR;

import java.io.Closeable;
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

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
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

import com.google.common.base.Function;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.configuration.QueryData;
import datawave.core.query.logic.BaseQueryLogic;
import datawave.core.query.logic.CheckpointableQueryLogic;
import datawave.core.query.logic.QueryCheckpoint;
import datawave.core.query.logic.QueryKey;
import datawave.core.query.logic.QueryLogicTransformer;
import datawave.data.type.Type;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
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
import datawave.util.TableName;
import datawave.webservice.query.exception.QueryException;

/**
 * Query Table implementation that accepts a single term and returns information from the global index for that term. The response includes the number of
 * occurrences of the term by type by day.
 */
public class ShardIndexQueryTable extends BaseQueryLogic<DiscoveredThing> implements CheckpointableQueryLogic {

    private static final Logger log = Logger.getLogger(ShardIndexQueryTable.class);
    protected ScannerFactory scannerFactory;
    private ShardIndexQueryConfiguration config;
    private MetadataHelperFactory metadataHelperFactory;

    public ShardIndexQueryTable() {}

    public ShardIndexQueryTable(ShardIndexQueryTable other) {
        super(other);
        this.config = ShardIndexQueryConfiguration.create(other);
    }

    @Override
    public ShardIndexQueryConfiguration getConfig() {
        if (this.config == null) {
            this.config = ShardIndexQueryConfiguration.create();
        }

        return this.config;
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
     * @param client
     *            the client
     * @param metadataTableName
     *            metadata table name
     * @param auths
     *            a set of auths
     * @return a new initialized MetadataHelper
     */
    protected MetadataHelper initializeMetadataHelper(AccumuloClient client, String metadataTableName, Set<Authorizations> auths) {
        return this.metadataHelperFactory.createMetadataHelper(client, metadataTableName, auths);
    }

    public MetadataHelperFactory getMetadataHelperFactory() {
        return metadataHelperFactory;
    }

    public void setMetadataHelperFactory(MetadataHelperFactory metadataHelperFactory) {
        this.metadataHelperFactory = metadataHelperFactory;
    }

    public void setMetadataTableName(String tableName) {
        getConfig().setMetadataTableName(tableName);
    }

    public String getMetadataTableName() {
        return getConfig().getMetadataTableName();
    }

    public String getIndexTableName() {
        return getConfig().getIndexTableName();
    }

    public void setIndexTableName(String indexTableName) {
        getConfig().setIndexTableName(indexTableName);
    }

    public String getReverseIndexTableName() {
        return getConfig().getReverseIndexTableName();
    }

    public void setReverseIndexTableName(String reverseIndexTableName) {
        getConfig().setReverseIndexTableName(reverseIndexTableName);
    }

    @Override
    public GenericQueryConfiguration initialize(AccumuloClient client, Query settings, Set<Authorizations> auths) throws Exception {
        this.config = new ShardIndexQueryConfiguration(this, settings);
        this.scannerFactory = new ScannerFactory(client);
        MetadataHelper metadataHelper = initializeMetadataHelper(client, getConfig().getMetadataTableName(), auths);

        if (StringUtils.isEmpty(settings.getQuery())) {
            throw new IllegalArgumentException("Query cannot be null");
        }

        if (log.isDebugEnabled()) {
            log.debug("Query parameters set to " + settings.getParameters());
        }

        String tModelName = getTrimmedOrNull(settings, QueryParameters.PARAMETER_MODEL_NAME);
        if (tModelName != null) {
            getConfig().setModelName(tModelName);
        }

        String tModelTableName = getTrimmedOrNull(settings, QueryParameters.PARAMETER_MODEL_TABLE_NAME);
        if (tModelTableName != null) {
            getConfig().setModelTableName(tModelTableName);
        }

        getConfig().setQueryModel(metadataHelper.getQueryModel(getConfig().getModelTableName(), getConfig().getModelName(), null));

        String datatypeFilterString = getTrimmedOrNull(settings, QueryParameters.DATATYPE_FILTER_SET);
        if (datatypeFilterString != null) {
            getConfig().setDatatypeFilter(new HashSet<>(Arrays.asList(datatypeFilterString.split(PARAM_VALUE_SEP_STR))));
            if (log.isDebugEnabled()) {
                log.debug("Data type filter set to " + getConfig().getDatatypeFilterAsString());
            }
        }

        getConfig().setClient(client);
        getConfig().setAuthorizations(auths);

        if (settings.getBeginDate() != null) {
            getConfig().setBeginDate(settings.getBeginDate());
        } else {
            getConfig().setBeginDate(new Date(0));
            if (log.isDebugEnabled()) {
                log.debug("No begin date supplied in settings.");
            }
        }

        if (settings.getEndDate() != null) {
            getConfig().setEndDate(settings.getEndDate());
        } else {
            getConfig().setEndDate(new Date(Long.MAX_VALUE));
            if (log.isDebugEnabled()) {
                log.debug("No end date supplied in settings.");
            }
        }

        // start with a trimmed version of the query, converted to JEXL
        LuceneToJexlQueryParser parser = new LuceneToJexlQueryParser();
        parser.setAllowLeadingWildCard(this.isAllowLeadingWildcard());
        QueryNode node = parser.parse(settings.getQuery().trim());
        // TODO: Validate that this is a simple list of terms type of query
        getConfig().setQueryString(node.getOriginalQuery());
        if (log.isDebugEnabled()) {
            log.debug("Original Query = " + settings.getQuery().trim());
            log.debug("JEXL Query = " + node.getOriginalQuery());
        }

        // Parse & flatten the query.
        ASTJexlScript origScript = JexlASTHelper.parseAndFlattenJexlQuery(getConfig().getQueryString());
        ASTJexlScript script;
        try {
            script = UnfieldedIndexExpansionVisitor.expandUnfielded(getConfig(), this.scannerFactory, metadataHelper, origScript);
        } catch (EmptyUnfieldedTermExpansionException e) {
            Multimap<String,String> emptyMap = Multimaps.unmodifiableMultimap(HashMultimap.create());
            getConfig().setNormalizedTerms(emptyMap);
            getConfig().setNormalizedPatterns(emptyMap);
            return getConfig();
        }

        Set<String> dataTypes = getConfig().getDatatypeFilter();
        Set<String> allFields = metadataHelper.getAllFields(dataTypes);

        script = QueryModelVisitor.applyModel(script, getConfig().getQueryModel(), allFields);
        if (log.isTraceEnabled()) {
            log.trace("fetching dataTypes from FetchDataTypesVisitor");
        }
        Multimap<String,Type<?>> fieldToDataTypeMap = FetchDataTypesVisitor.fetchDataTypes(metadataHelper, getConfig().getDatatypeFilter(), script);
        getConfig().setDataTypes(fieldToDataTypeMap);
        getConfig().setQueryFieldsDatatypes(fieldToDataTypeMap);

        final Set<String> indexedFields = metadataHelper.getIndexedFields(dataTypes);
        getConfig().setIndexedFields(indexedFields);

        final Set<String> reverseIndexedFields = metadataHelper.getReverseIndexedFields(dataTypes);
        getConfig().setReverseIndexedFields(reverseIndexedFields);

        final Multimap<String,Type<?>> normalizedFields = metadataHelper.getFieldsToDatatypes(dataTypes);
        getConfig().setNormalizedFieldsDatatypes(normalizedFields);

        if (log.isTraceEnabled()) {
            log.trace("Normalizers:");
            for (String field : fieldToDataTypeMap.keySet()) {
                log.trace(field + ": " + fieldToDataTypeMap.get(field));
            }
        }

        script = ExpandMultiNormalizedTerms.expandTerms(getConfig(), metadataHelper, script);

        Multimap<String,String> literals = LiteralNodeVisitor.getLiterals(script);
        Multimap<String,String> patterns = PatternNodeVisitor.getPatterns(script);
        Map<Entry<String,String>,Range> rangesForTerms = Maps.newHashMap();
        Map<Entry<String,String>,Entry<Range,Boolean>> rangesForPatterns = Maps.newHashMap();

        getConfig().setNormalizedTerms(literals);
        getConfig().setNormalizedPatterns(patterns);

        if (log.isDebugEnabled()) {
            log.debug("Normalized Literals = " + literals);
            log.debug("Normalized Patterns = " + patterns);
        }

        for (Entry<String,String> entry : literals.entries()) {
            rangesForTerms.put(entry, ShardIndexQueryTableStaticMethods.getLiteralRange(entry));

        }
        for (Entry<String,String> entry : patterns.entries()) {
            ShardIndexQueryTableStaticMethods.RefactoredRangeDescription r = ShardIndexQueryTableStaticMethods.getRegexRange(entry, isFullTableScanEnabled(),
                            metadataHelper, getConfig());

            rangesForPatterns.put(entry, Maps.immutableEntry(r.range, r.isForReverseIndex));
        }

        getConfig().setRangesForTerms(rangesForTerms);
        getConfig().setRangesForPatterns(rangesForPatterns);

        getConfig().setQueries(createQueries(getConfig()));

        return getConfig();
    }

    public List<QueryData> createQueries(ShardIndexQueryConfiguration config) throws QueryException, TableNotFoundException, IOException, ExecutionException {
        final List<QueryData> queries = Lists.newLinkedList();

        for (Entry<String,String> termEntry : getConfig().getNormalizedTerms().entries()) {
            String query = termEntry.getKey();
            Range range = getConfig().getRangesForTerms().get(termEntry);
            List<IteratorSetting> settings = getIteratorSettingsForDiscovery(getConfig(), Collections.singleton(termEntry.getValue()), Collections.emptySet(),
                            getConfig().getTableName().equals(getConfig().getReverseIndexTableName()), false);
            List<String> cfs = ShardIndexQueryTableStaticMethods.getColumnFamilies(getConfig(), false, Collections.singleton(termEntry.getKey()));
            queries.add(new QueryData(config.getIndexTableName(), query, Collections.singleton(range), settings, cfs));
        }

        for (Entry<String,String> patternEntry : getConfig().getNormalizedPatterns().entries()) {
            String query = patternEntry.getKey();
            Entry<Range,Boolean> rangeEntry = getConfig().getRangesForPatterns().get(patternEntry);
            String tName = rangeEntry.getValue() ? TableName.SHARD_RINDEX : TableName.SHARD_INDEX;
            List<IteratorSetting> settings = getIteratorSettingsForDiscovery(getConfig(), Collections.emptySet(),
                            Collections.singleton(patternEntry.getValue()), rangeEntry.getValue(), false);
            List<String> cfs = ShardIndexQueryTableStaticMethods.getColumnFamilies(getConfig(), rangeEntry.getValue(),
                            Collections.singleton(patternEntry.getKey()));
            queries.add(new QueryData(tName, query, Collections.singleton(rangeEntry.getKey()), settings, cfs));
        }
        return queries;
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

        this.config = (ShardIndexQueryConfiguration) genericConfig;
        final List<Entry<BatchScanner,QueryData>> batchscanners = Lists.newLinkedList();

        for (QueryData qd : config.getQueries()) {
            // scan the table
            BatchScanner bs = scannerFactory.newScanner(qd.getTableName(), config.getAuthorizations(), config.getNumQueryThreads(), config.getQuery());

            bs.setRanges(qd.getRanges());
            for (IteratorSetting setting : qd.getSettings()) {
                bs.addScanIterator(setting);
            }

            for (String cf : qd.getColumnFamilies()) {
                bs.fetchColumnFamily(new Text(cf));
            }

            batchscanners.add(Maps.immutableEntry(bs, qd));
        }

        final Iterator<Entry<BatchScanner,QueryData>> batchScannerIterator = batchscanners.iterator();

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
    }

    /**
     * Implementations use the configuration to setup execution of a portion of their query. getTransformIterator should be used to get the partial results if
     * any.
     *
     * @param client
     *            The accumulo client
     * @param baseConfig
     *            The shard query configuration
     * @param checkpoint
     */
    @Override
    public void setupQuery(AccumuloClient client, GenericQueryConfiguration baseConfig, QueryCheckpoint checkpoint) throws Exception {
        ShardIndexQueryConfiguration config = (ShardIndexQueryConfiguration) baseConfig;
        baseConfig.setQueries(checkpoint.getQueries());
        config.setClient(client);

        scannerFactory = new ScannerFactory(client);
        MetadataHelper metadataHelper = initializeMetadataHelper(client, config.getMetadataTableName(), config.getAuthorizations());
        config.setQueryModel(metadataHelper.getQueryModel(config.getModelTableName(), config.getModelName(), null));

        setupQuery(config);
    }

    @Override
    public boolean isCheckpointable() {
        return getConfig().isCheckpointable();
    }

    @Override
    public void setCheckpointable(boolean checkpointable) {
        getConfig().setCheckpointable(checkpointable);
    }

    /**
     * This can be called at any point to get a checkpoint such that this query logic instance can be torn down to be rebuilt later. At a minimum this should be
     * called after the getTransformIterator is depleted of results.
     *
     * @param queryKey
     *            The query key to include in the checkpoint
     * @return The query checkpoint
     */
    @Override
    public List<QueryCheckpoint> checkpoint(QueryKey queryKey) {
        if (!isCheckpointable()) {
            throw new UnsupportedOperationException("Cannot checkpoint a query that is not checkpointable.  Try calling setCheckpointable(true) first.");
        }

        // if we have started returning results, then capture the state of the query data objects
        if (this.iterator != null) {
            List<QueryCheckpoint> checkpoints = Lists.newLinkedList();
            for (QueryData qd : getConfig().getQueries()) {
                checkpoints.add(new QueryCheckpoint(queryKey, Collections.singletonList(qd)));
            }
            return checkpoints;
        }
        // otherwise we still need to plan or there are no results
        else {
            return Lists.newArrayList(new QueryCheckpoint(queryKey));
        }
    }

    @Override
    public QueryCheckpoint updateCheckpoint(QueryCheckpoint checkpoint) {
        // for the shard index query logic, the query data objects automatically get update with
        // the last result returned, so the checkpoint should already be updated!
        return checkpoint;
    }

    public static List<IteratorSetting> getIteratorSettingsForDiscovery(ShardQueryConfiguration config, Collection<String> literals,
                    Collection<String> patterns, boolean reverseIndex, boolean uniqueTermsOnly) {

        // The begin date from the query may be down to the second, for doing lookups in the index we want to use the day because
        // the times in the index table have been truncated to the day.
        Date begin = DateUtils.truncate(config.getBeginDate(), Calendar.DAY_OF_MONTH);
        // we don't need to bump up the end date any more because it's not apart of the range set on the scanner
        Date end = config.getEndDate();

        LongRange dateRange = new LongRange(begin.getTime(), end.getTime());

        List<IteratorSetting> settings = Lists.newLinkedList();
        settings.add(ShardIndexQueryTableStaticMethods.configureGlobalIndexDateRangeFilter(config, dateRange));
        settings.add(ShardIndexQueryTableStaticMethods.configureGlobalIndexDataTypeFilter(config, config.getDatatypeFilter()));

        settings.add(ShardIndexQueryTableStaticMethods.configureGlobalIndexTermMatchingIterator(config, literals, patterns, reverseIndex, uniqueTermsOnly));

        settings.add(new IteratorSetting(config.getBaseIteratorPriority() + 50, DiscoveryIterator.class));
        return settings;
    }

    public boolean isFullTableScanEnabled() {
        return getConfig().getFullTableScanEnabled();
    }

    public void setFullTableScanEnabled(boolean fullTableScanEnabled) {
        this.getConfig().setFullTableScanEnabled(fullTableScanEnabled);
    }

    public boolean isAllowLeadingWildcard() {
        return getConfig().isAllowLeadingWildcard();
    }

    public void setAllowLeadingWildcard(boolean allowLeadingWildcard) {
        getConfig().setAllowLeadingWildcard(allowLeadingWildcard);
    }

    @Override
    public AccumuloConnectionFactory.Priority getConnectionPriority() {
        return AccumuloConnectionFactory.Priority.NORMAL;
    }

    @Override
    public QueryLogicTransformer getTransformer(Query settings) {
        return new DiscoveryTransformer(this, settings, getConfig().getQueryModel());
    }

    /**
     * Query model
     *
     * @return the model name
     */
    public String getModelName() {
        return getConfig().getModelName();
    }

    public void setModelName(String modelName) {
        getConfig().setModelName(modelName);
    }

    public String getModelTableName() {
        return getConfig().getModelTableName();
    }

    public void setModelTableName(String modelTableName) {
        getConfig().setModelTableName(modelTableName);
    }

    public QueryModel getQueryModel() {
        return getConfig().getQueryModel();
    }

    public void setQueryModel(QueryModel model) {
        getConfig().setQueryModel(model);
    }

    public class CloseableIterator implements Iterator<Entry<Key,Value>>, Closeable {

        private final Iterator<Entry<BatchScanner,QueryData>> batchScannerIterator;

        protected Boolean reverseIndex = false;
        protected Entry<Key,Value> currentEntry = null;
        protected BatchScanner currentBS = null;
        protected Iterator<Entry<Key,Value>> currentIter = null;
        protected QueryData queryData = null;

        protected volatile boolean closed = false;

        public CloseableIterator(Iterator<Entry<BatchScanner,QueryData>> batchScannerIterator) {
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
                Entry<BatchScanner,QueryData> entry = batchScannerIterator.next();
                this.currentBS = entry.getKey();
                this.queryData = entry.getValue();
                this.reverseIndex = entry.getValue().getTableName().equals(getConfig().getReverseIndexTableName());
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

                queryData.setLastResult(cur.getKey());

                if (this.reverseIndex) {
                    Text term = new Text((new StringBuilder(cur.getKey().getRow().toString())).reverse().toString());
                    cur = Maps.immutableEntry(new Key(term, cur.getKey().getColumnFamily(), cur.getKey().getColumnQualifier(),
                                    cur.getKey().getColumnVisibility(), cur.getKey().getTimestamp()), cur.getValue());
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

        @Override
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
        optionalParams.add(datawave.microservice.query.QueryParameters.QUERY_BEGIN);
        optionalParams.add(datawave.microservice.query.QueryParameters.QUERY_END);
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
        return getConfig().getRealmSuffixExclusionPatterns();
    }

    public void setRealmSuffixExclusionPatterns(List<String> realmSuffixExclusionPatterns) {
        getConfig().setRealmSuffixExclusionPatterns(realmSuffixExclusionPatterns);
    }

}
