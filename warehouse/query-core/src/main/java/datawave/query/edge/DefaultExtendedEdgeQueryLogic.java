package datawave.query.edge;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.jexl3.JexlException;
import org.apache.commons.jexl3.parser.ParseException;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import datawave.audit.SelectorExtractor;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.Type;
import datawave.edge.util.EdgeKeyUtil;
import datawave.query.config.EdgeExtendedSummaryConfiguration;
import datawave.query.config.EdgeQueryConfiguration;
import datawave.query.iterator.filter.EdgeFilterIterator;
import datawave.query.tables.edge.EdgeQueryLogic;
import datawave.query.tables.edge.contexts.VisitationContext;
import datawave.query.transformer.EdgeQueryTransformer;
import datawave.query.util.MetadataHelper;
import datawave.util.StringUtils;
import datawave.webservice.query.Query;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.configuration.QueryData;
import datawave.webservice.query.logic.QueryLogicTransformer;

public class DefaultExtendedEdgeQueryLogic extends EdgeQueryLogic {

    private static final Logger log = Logger.getLogger(DefaultExtendedEdgeQueryLogic.class);

    protected boolean summaryInputType = false;
    protected boolean summaryOutputType = false;
    protected boolean allowOverrideIO = true;

    protected SelectorExtractor listSelectorExtractor;

    public DefaultExtendedEdgeQueryLogic() {
        super();
    }

    public DefaultExtendedEdgeQueryLogic(DefaultExtendedEdgeQueryLogic logic) {
        super(logic);
        summaryInputType = logic.isSummaryInputType();
        summaryOutputType = logic.isSummaryOutputType();
        allowOverrideIO = logic.isAllowOverrideIO();
    }

    @Override
    public DefaultExtendedEdgeQueryLogic clone() {
        return new DefaultExtendedEdgeQueryLogic(this);
    }

    @Override
    protected EdgeQueryConfiguration setUpConfig(Query settings) {
        return new EdgeExtendedSummaryConfiguration(this, settings).parseParameters(settings);
    }

    @Override
    public void setupQuery(GenericQueryConfiguration configuration) throws Exception {
        EdgeExtendedSummaryConfiguration localConf = (EdgeExtendedSummaryConfiguration) configuration;

        config = (EdgeExtendedSummaryConfiguration) configuration;
        prefilterValues = null;
        EdgeExtendedSummaryConfiguration.dateType dateFilterType = localConf.getDateRangeType();

        if (log.isTraceEnabled()) {
            log.trace("Performing edge table query: " + config.getQueryString());
        }

        // TODO check to see if overriding I/O necessary
        if (allowOverrideIO && localConf.isOverRideInput()) {
            this.summaryInputType = localConf.isSummaryInputType();
        }
        if (allowOverrideIO && localConf.isOverRideOutput()) {
            this.summaryOutputType = localConf.isAggregateResults();
        }

        boolean includeStats = localConf.includeStats();

        String queryString = config.getQueryString();

        MetadataHelper metadataHelper = super.prepareMetadataHelper(config.getClient(), config.getModelTableName(), config.getAuthorizations());

        loadQueryModel(metadataHelper, config);

        // Don't apply model if this.summaryInputType == true, which indicates that
        // query.syntax parameter equals "LIST", meaning that the query string is just a
        // list of source values...no field names to translate
        if (this.summaryInputType == false) {
            queryString = applyQueryModel(queryString);
        }

        // set the modified queryString back into the config, for easy access
        config.setQueryString(queryString);

        String normalizedQuery = "";
        String statsNormalizedQuery = "";

        QueryData qData = configureRanges(queryString);
        setRanges(qData.getRanges());

        VisitationContext context = null;
        if (this.summaryInputType == false) {
            try {
                context = normalizeJexlQuery(queryString, false);
                normalizedQuery = context.getNormalizedQuery().toString();
                statsNormalizedQuery = context.getNormalizedStatsQuery().toString();
                if (log.isTraceEnabled()) {
                    log.trace("Jexl after normalizing both vertices: " + normalizedQuery);
                }
            } catch (JexlException ex) {
                try {
                    log.error("Error parsing user query.", ex);
                } catch (Exception ex2) {
                    log.error("Exception thrown by logger (???)");
                }
            }
        }

        if ((null == normalizedQuery || normalizedQuery.equals("")) && qData.getRanges().size() < 1) {
            throw new IllegalStateException("Query string is empty after initial processing, no ranges or filters can be generated to execute.");
        }

        addIterators(qData, getDateBasedIterators(config.getBeginDate(), config.getEndDate(), currentIteratorPriority, dateFilterSkipLimit, dateFilterScanLimit,
                        dateFilterType));

        if (!normalizedQuery.equals("")) {
            if (log.isTraceEnabled()) {
                log.trace("Query being sent to the filter iterator: " + normalizedQuery);
            }
            IteratorSetting edgeIteratorSetting = new IteratorSetting(currentIteratorPriority,
                            EdgeFilterIterator.class.getSimpleName() + "_" + currentIteratorPriority, EdgeFilterIterator.class);
            edgeIteratorSetting.addOption(EdgeFilterIterator.JEXL_OPTION, normalizedQuery);
            edgeIteratorSetting.addOption(EdgeFilterIterator.PROTOBUF_OPTION, "TRUE");

            if (!statsNormalizedQuery.equals("")) {
                edgeIteratorSetting.addOption(EdgeFilterIterator.JEXL_STATS_OPTION, statsNormalizedQuery);
            }
            if (prefilterValues != null) {
                String value = serializePrefilter();
                edgeIteratorSetting.addOption(EdgeFilterIterator.PREFILTER_ALLOWLIST, value);
            }

            if (includeStats) {
                edgeIteratorSetting.addOption(EdgeFilterIterator.INCLUDE_STATS_OPTION, "TRUE");
            } else {
                edgeIteratorSetting.addOption(EdgeFilterIterator.INCLUDE_STATS_OPTION, "FALSE");
            }

            addIterator(qData, edgeIteratorSetting);
        }

        if (log.isTraceEnabled()) {
            log.trace("Configuring connection: tableName: " + config.getTableName() + ", auths: " + config.getAuthorizations());
        }

        BatchScanner scanner = createBatchScanner(config);

        if (log.isTraceEnabled()) {
            log.trace("Using the following ranges: " + qData.getRanges());
        }

        if (context != null && context.isHasAllCompleteColumnFamilies()) {
            for (Text columnFamily : context.getColumnFamilies()) {
                scanner.fetchColumnFamily(columnFamily);
            }

        }
        scanner.setRanges(qData.getRanges());

        addCustomFilters(qData, currentIteratorPriority);

        for (IteratorSetting setting : qData.getSettings()) {
            scanner.addScanIterator(setting);
        }

        this.scanner = scanner;
        iterator = scanner.iterator();
    }

    @Override
    protected QueryData configureRanges(String queryString) throws ParseException {
        QueryData qData = new QueryData();
        if (this.summaryInputType) {
            Set<Range> ranges = computeRanges((EdgeExtendedSummaryConfiguration) this.config);
            qData.setRanges(ranges);
            return qData;
        } else {
            return super.configureRanges(queryString);
        }
    }

    protected Set<Range> computeRanges(EdgeExtendedSummaryConfiguration configuration) {
        Set<Range> ranges = new HashSet<>();
        String query = configuration.getQueryString();

        String[] sources = StringUtils.split(query, configuration.getDelimiter());
        for (String source : sources) {
            for (String normalizedSource : normalizeQualifiedSource(source)) {
                ranges.add(EdgeKeyUtil.createEscapedRange(normalizedSource, false, configuration.includeStats(), configuration.isIncludeRelationships()));
            }
        }
        return ranges;
    }

    protected Collection<String> normalizeQualifiedSource(String qualifiedSource) {

        int qualifierStart = qualifiedSource.lastIndexOf('<');
        String source = qualifiedSource;
        String normalizedQualifier = "";
        if (qualifierStart > 0) {
            source = qualifiedSource.substring(0, qualifierStart);
            normalizedQualifier = qualifiedSource.substring(qualifierStart).toLowerCase();
        }
        Set<String> sources = new LinkedHashSet<>();
        List<? extends Type<?>> dataTypes = getDataTypes();
        if (dataTypes == null) {
            dataTypes = Arrays.asList((Type<?>) new LcNoDiacriticsType());
        }
        for (Type<?> type : dataTypes) {
            try {
                String normalizedSource = type.normalize(source);
                if (normalizedSource == null || "".equals(normalizedSource.trim())) {
                    continue;
                }
                String normalizedQualifiedSource = normalizedSource + normalizedQualifier;
                sources.add(normalizedQualifiedSource);
            } catch (Exception e) {
                // ignore -- couldn't normalize with this normalizer
            }
        }
        return sources;
    }

    @Override
    public QueryLogicTransformer getTransformer(Query settings) {
        return new EdgeQueryTransformer(settings, this.markingFunctions, this.responseObjectFactory);
    }

    @Override
    public List<String> getSelectors(Query settings) throws IllegalArgumentException {
        EdgeExtendedSummaryConfiguration conf = (EdgeExtendedSummaryConfiguration) setUpConfig(settings);
        List<String> selectorList = null;
        SelectorExtractor selExtr;

        if (conf.isSummaryInputType()) {
            selExtr = listSelectorExtractor;
        } else {
            selExtr = selectorExtractor;
        }

        if (selExtr != null) {
            try {
                selectorList = selExtr.extractSelectors(settings);
            } catch (Exception e) {
                throw new IllegalArgumentException(e.getMessage(), e);
            }
        }
        return selectorList;
    }

    @Override
    public Set<String> getOptionalQueryParameters() {
        Set<String> optionalParams = super.getOptionalQueryParameters();
        optionalParams.add(EdgeExtendedSummaryConfiguration.EDGE_TYPES_PARAM);
        return optionalParams;
    }

    public boolean isSummaryInputType() {
        return summaryInputType;
    }

    public void setSummaryInputType(boolean summaryInputType) {
        this.summaryInputType = summaryInputType;
    }

    public boolean isSummaryOutputType() {
        return summaryOutputType;
    }

    public void setSummaryOutputType(boolean summaryOutputType) {
        this.summaryOutputType = summaryOutputType;
    }

    public boolean isAllowOverrideIO() {
        return allowOverrideIO;
    }

    public void setAllowOverrideIO(boolean allowOverrideIO) {
        this.allowOverrideIO = allowOverrideIO;
    }

    public void setListSelectorExtractor(SelectorExtractor listSelectorExtractor) {
        this.listSelectorExtractor = listSelectorExtractor;
    }
}
