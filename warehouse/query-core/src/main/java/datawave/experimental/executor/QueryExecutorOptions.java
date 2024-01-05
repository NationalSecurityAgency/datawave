package datawave.experimental.executor;

import static datawave.query.iterator.QueryOptions.FULL_TABLE_SCAN_ONLY;
import static datawave.query.iterator.QueryOptions.GROUP_FIELDS;
import static datawave.query.iterator.QueryOptions.GROUP_FIELDS_BATCH_SIZE;
import static datawave.query.iterator.QueryOptions.LIMIT_FIELDS;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import datawave.query.common.grouping.GroupFields;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.function.JexlEvaluation;
import datawave.query.function.LimitFields;
import datawave.query.iterator.QueryOptions;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.util.TypeMetadata;
import datawave.webservice.query.configuration.QueryData;

/**
 * The result of merging the ShardQueryConfig with QueryData. Acts as the QueryOptions for the QueryExecutor.
 */
public class QueryExecutorOptions {

    // these are really execution options
    private boolean uidParallelScan = true;
    private boolean uidSequentialScan = false;

    // enable limited key filtering when aggregating documents
    private boolean configuredDocumentScan = false;

    private boolean tfSequentialScan = false;
    // uses a custom TF iterator to perform server-side filtering
    private boolean tfConfiguredScan = true;
    // use a custom TF iterator to perform server-side filtering with a seeking scan
    private boolean tfSeekingConfiguredScan = false;

    // record and aggregate shard stats
    private boolean statsEnabled = false;
    // log summary stats for each stage (FI, Event, TF)
    private boolean logStageSummaryStats = false;
    // log summary stats for each shard
    private boolean logShardSummaryStats = false;

    private Range range;
    private String query;
    private ASTJexlScript script;

    private Set<String> indexedFields;
    private Set<String> indexOnlyFields;
    private Set<String> termFrequencyFields;

    private TypeMetadata typeMetadata;

    private GroupFields groupFields = new GroupFields();
    private int groupFieldsBatchSize = Integer.MAX_VALUE;

    private LimitFields limitFields = null;
    private final Map<String,Integer> limitFieldsMap = new HashMap<>();

    private Set<String> includeFields;
    private Set<String> excludeFields;

    // options from ShardQueryConfig
    private String tableName;
    private AccumuloClient client;
    private Authorizations auths;

    public QueryExecutorOptions() {
        // for bean configuration
    }

    /**
     * Load configurations from a QueryData object
     *
     * @param data
     *            a QueryData
     */
    public void configureViaQueryData(QueryData data) {
        boolean isFullTableScan;
        if (data.getRanges().size() != 1) {
            throw new IllegalStateException("QueryExecutor operates on one range at a time, was provided: " + data.getRanges().size() + " ranges");
        }
        range = data.getRanges().iterator().next();

        query = data.getQuery();

        try {
            script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        } catch (ParseException e) {
            throw new RuntimeException("Could not parse query: " + query, e);
        }

        if (data.getSettings().size() != 1) {
            throw new IllegalStateException("QueryExecutor expects one iterator setting but got " + data.getSettings().size());
        }

        IteratorSetting setting = data.getSettings().get(0);
        Map<String,String> options = setting.getOptions();

        indexedFields = getFields(QueryOptions.INDEXED_FIELDS, options);
        indexOnlyFields = getFields(QueryOptions.INDEX_ONLY_FIELDS, options);
        termFrequencyFields = getFields(QueryOptions.TERM_FREQUENCY_FIELDS, options);

        if (options.containsKey(QueryOptions.TYPE_METADATA)) {
            String option = options.get(QueryOptions.TYPE_METADATA);
            if (options.containsKey(QueryOptions.QUERY_MAPPING_COMPRESS)) {
                try {
                    option = QueryOptions.decompressOption(option, QueryOptions.UTF8);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                if (option == null) {
                    // if option is null then we actually had a deserialized type metadata object
                    option = options.get(QueryOptions.TYPE_METADATA);
                }
            }
            typeMetadata = new TypeMetadata(option);
        } else {
            throw new DatawaveFatalQueryException("QueryExecutor did not have a TypeMetadata option");
        }

        if (options.containsKey(GROUP_FIELDS)) {
            groupFields = GroupFields.from(options.get(GROUP_FIELDS));
        }

        if (options.containsKey(GROUP_FIELDS_BATCH_SIZE)) {
            String option = options.get(GROUP_FIELDS_BATCH_SIZE);
            groupFieldsBatchSize = Integer.parseInt(option);
        }

        // grouping is going to be interesting within a webserver-side context

        if (options.containsKey(LIMIT_FIELDS)) {
            String option = options.get(LIMIT_FIELDS);
            for (String paramGroup : Splitter.on(',').omitEmptyStrings().trimResults().split(option)) {
                String[] keyAndValue = Iterables.toArray(Splitter.on('=').omitEmptyStrings().trimResults().split(paramGroup), String.class);
                if (keyAndValue.length > 1) {
                    limitFieldsMap.put(keyAndValue[0], Integer.parseInt(keyAndValue[1]));
                }
            }

            limitFields = new LimitFields(limitFieldsMap, Collections.emptySet());
        }

        if (options.containsKey(FULL_TABLE_SCAN_ONLY)) {
            isFullTableScan = Boolean.parseBoolean(options.get(FULL_TABLE_SCAN_ONLY));
            if (isFullTableScan) {
                throw new IllegalStateException("QueryExecutor does not support full table scans");
            }
        }

        // RETURN_FIELDS should be used here. HIT_TERM should be added already.
        if (options.containsKey(QueryOptions.PROJECTION_FIELDS)) {
            String option = options.get(QueryOptions.PROJECTION_FIELDS);
            includeFields = new HashSet<>(Arrays.asList(StringUtils.split(option, ',')));
            if (!includeFields.isEmpty()) {
                includeFields.add(JexlEvaluation.HIT_TERM_FIELD);
            }
        }

        if (options.containsKey(QueryOptions.DISALLOWLISTED_FIELDS)) {
            String option = options.get(QueryOptions.DISALLOWLISTED_FIELDS);
            excludeFields = new HashSet<>(Arrays.asList(StringUtils.split(option, ',')));
        }
    }

    private Set<String> getFields(String key, Map<String,String> options) {
        if (options.containsKey(key)) {
            String option = options.get(key);
            if (!StringUtils.isEmpty(option)) {
                return Sets.newHashSet(StringUtils.split(option, ','));
            }
        }
        return Collections.emptySet();
    }

    public Range getRange() {
        return this.range;
    }

    public String getQuery() {
        return this.query;
    }

    public ASTJexlScript getScript() {
        return this.script;
    }

    public Set<String> getIndexedFields() {
        return this.indexedFields;
    }

    public Set<String> getIndexOnlyFields() {
        return this.indexOnlyFields;
    }

    public Set<String> getTermFrequencyFields() {
        return this.termFrequencyFields;
    }

    public TypeMetadata getTypeMetadata() {
        return this.typeMetadata;
    }

    public GroupFields getGroupFields() {
        return this.groupFields;
    }

    public int getGroupFieldsBatchSize() {
        return this.groupFieldsBatchSize;
    }

    public LimitFields getLimitFields() {
        return this.limitFields;
    }

    public boolean isUidParallelScan() {
        return uidParallelScan;
    }

    public void setUidParallelScan(boolean uidParallelScan) {
        this.uidParallelScan = uidParallelScan;
    }

    public boolean isUidSequentialScan() {
        return uidSequentialScan;
    }

    public void setUidSequentialScan(boolean uidSequentialScan) {
        this.uidSequentialScan = uidSequentialScan;
    }

    public boolean isTfConfiguredScan() {
        return tfConfiguredScan;
    }

    public void setTfConfiguredScan(boolean tfConfiguredScan) {
        this.tfConfiguredScan = tfConfiguredScan;
    }

    public boolean isTfSequentialScan() {
        return tfSequentialScan;
    }

    public void setTfSequentialScan(boolean tfSequentialScan) {
        this.tfSequentialScan = tfSequentialScan;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public AccumuloClient getClient() {
        return client;
    }

    public void setClient(AccumuloClient client) {
        this.client = client;
    }

    public Authorizations getAuths() {
        return auths;
    }

    public void setAuths(Authorizations auths) {
        this.auths = auths;
    }

    public boolean isTfSeekingConfiguredScan() {
        return tfSeekingConfiguredScan;
    }

    public void setTfSeekingConfiguredScan(boolean tfSeekingConfiguredScan) {
        this.tfSeekingConfiguredScan = tfSeekingConfiguredScan;
    }

    public boolean isStatsEnabled() {
        return statsEnabled;
    }

    public void setStatsEnabled(boolean statsEnabled) {
        this.statsEnabled = statsEnabled;
    }

    public Set<String> getIncludeFields() {
        return includeFields;
    }

    public void setIncludeFields(Set<String> includeFields) {
        this.includeFields = includeFields;
    }

    public Set<String> getExcludeFields() {
        return excludeFields;
    }

    public void setExcludeFields(Set<String> excludeFields) {
        this.excludeFields = excludeFields;
    }

    public boolean isConfiguredDocumentScan() {
        return configuredDocumentScan;
    }

    public void setConfiguredDocumentScan(boolean configuredDocumentScan) {
        this.configuredDocumentScan = configuredDocumentScan;
    }

    public boolean isLogStageSummaryStats() {
        return logStageSummaryStats;
    }

    public void setLogStageSummaryStats(boolean logStageSummaryStats) {
        this.logStageSummaryStats = logStageSummaryStats;
    }

    public boolean isLogShardSummaryStats() {
        return logShardSummaryStats;
    }

    public void setLogShardSummaryStats(boolean logShardSummaryStats) {
        this.logShardSummaryStats = logShardSummaryStats;
    }
}
