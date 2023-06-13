package datawave.query.edge;

import datawave.audit.SelectorExtractor;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.configuration.QueryData;
import datawave.core.query.logic.QueryLogicTransformer;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.Type;
import datawave.edge.util.EdgeKeyUtil;
import datawave.query.config.EdgeExtendedSummaryConfiguration;
import datawave.query.iterator.filter.EdgeFilterIterator;
import datawave.query.tables.ScannerFactory;
import datawave.query.tables.edge.EdgeQueryLogic;
import datawave.query.tables.edge.contexts.VisitationContext;
import datawave.query.transformer.EdgeQueryTransformer;
import datawave.query.util.MetadataHelper;
import datawave.util.StringUtils;
import datawave.webservice.query.Query;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.jexl3.JexlException;
import org.apache.commons.jexl3.parser.ParseException;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

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
        
        // Set EdgeQueryConfiguration variables
        this.config = EdgeExtendedSummaryConfiguration.create(logic);
        
        summaryInputType = logic.isSummaryInputType();
        summaryOutputType = logic.isSummaryOutputType();
        allowOverrideIO = logic.isAllowOverrideIO();
        listSelectorExtractor = logic.listSelectorExtractor;
    }
    
    @Override
    public EdgeExtendedSummaryConfiguration getConfig() {
        if (config == null) {
            config = new EdgeExtendedSummaryConfiguration();
        }
        return (EdgeExtendedSummaryConfiguration) config;
    }
    
    @Override
    public DefaultExtendedEdgeQueryLogic clone() {
        return new DefaultExtendedEdgeQueryLogic(this);
    }
    
    @Override
    public GenericQueryConfiguration initialize(AccumuloClient client, Query settings, Set<Authorizations> auths) throws Exception {
        currentIteratorPriority = super.getBaseIteratorPriority() + 30;
        
        EdgeExtendedSummaryConfiguration config = getConfig().parseParameters(settings);
        
        config.setClient(client);
        config.setAuthorizations(auths);
        
        String queryString = getJexlQueryString(settings);
        
        if (null == queryString) {
            throw new IllegalArgumentException("Query cannot be null");
        } else {
            config.setQueryString(queryString);
        }
        config.setBeginDate(settings.getBeginDate());
        config.setEndDate(settings.getEndDate());
        
        scannerFactory = new ScannerFactory(client);
        
        prefilterValues = null;
        EdgeExtendedSummaryConfiguration.dateType dateFilterType = config.getDateRangeType();
        
        log.debug("Performing edge table query: " + config.getQueryString());
        
        // TODO check to see if overriding I/O necessary
        if (allowOverrideIO && config.isOverRideInput()) {
            this.summaryInputType = config.isSummaryInputType();
        }
        if (allowOverrideIO && config.isOverRideOutput()) {
            this.summaryOutputType = config.isAggregateResults();
        }
        
        boolean includeStats = config.includeStats();
        
        MetadataHelper metadataHelper = super.prepareMetadataHelper(config.getClient(), config.getMetadataTableName(), config.getAuthorizations());
        
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
        
        Set<Range> ranges = configureRanges(queryString);
        
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
                log.error("Error parsing user query.", ex);
            }
        }
        
        if ((null == normalizedQuery || normalizedQuery.equals("")) && ranges.size() < 1) {
            throw new IllegalStateException("Query string is empty after initial processing, no ranges or filters can be generated to execute.");
        }
        
        QueryData qData = new QueryData();
        qData.setTableName(config.getTableName());
        qData.setRanges(ranges);
        
        addIterators(qData, getDateBasedIterators(config.getBeginDate(), config.getEndDate(), currentIteratorPriority, config.getDateFilterSkipLimit(),
                        config.getDateFilterScanLimit(), dateFilterType));
        
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
                edgeIteratorSetting.addOption(EdgeFilterIterator.PREFILTER_WHITELIST, value);
            }
            
            if (includeStats) {
                edgeIteratorSetting.addOption(EdgeFilterIterator.INCLUDE_STATS_OPTION, "TRUE");
            } else {
                edgeIteratorSetting.addOption(EdgeFilterIterator.INCLUDE_STATS_OPTION, "FALSE");
            }
            
            addIterator(qData, edgeIteratorSetting);
        }
        
        if (context != null && context.isHasAllCompleteColumnFamilies()) {
            for (Text columnFamily : context.getColumnFamilies()) {
                qData.addColumnFamily(columnFamily);
            }
        }
        
        addCustomFilters(qData, currentIteratorPriority);
        
        config.setQueries(Collections.singletonList(qData));
        
        return config;
    }
    
    @Override
    protected Set<Range> configureRanges(String queryString) throws ParseException {
        if (this.summaryInputType) {
            Set<Range> ranges = computeRanges((EdgeExtendedSummaryConfiguration) this.config);
            return ranges;
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
        return new EdgeQueryTransformer(settings, this.markingFunctions, this.responseObjectFactory, this.getEdgeFields());
    }
    
    @Override
    public List<String> getSelectors(Query settings) throws IllegalArgumentException {
        EdgeExtendedSummaryConfiguration conf = new EdgeExtendedSummaryConfiguration().parseParameters(settings);
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
