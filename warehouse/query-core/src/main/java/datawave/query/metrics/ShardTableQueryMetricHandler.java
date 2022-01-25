package datawave.query.metrics;

import java.io.IOException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import datawave.configuration.DatawaveEmbeddedProjectStageHolder;
import datawave.data.hash.UID;
import datawave.data.hash.UIDBuilder;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.config.RawRecordContainerImpl;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.mapreduce.handler.shard.AbstractColumnBasedHandler;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.writer.LiveContextWriter;
import datawave.ingest.table.config.TableConfigHelper;
import datawave.marking.MarkingFunctions;
import datawave.query.iterator.QueryOptions;
import datawave.query.map.SimpleQueryGeometryHandler;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.util.AuthorizationsUtil;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.common.connection.AccumuloConnectionFactory.Priority;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.QueryImpl.Parameter;
import datawave.webservice.query.cache.QueryMetricFactory;
import datawave.webservice.query.cache.ResultsPage;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.exception.QueryExceptionType;
import datawave.webservice.query.logic.QueryLogic;
import datawave.webservice.query.logic.QueryLogicFactory;
import datawave.webservice.query.metric.BaseQueryMetric;
import datawave.webservice.query.metric.BaseQueryMetric.PageMetric;
import datawave.webservice.query.metric.BaseQueryMetric.Lifecycle;
import datawave.webservice.query.metric.BaseQueryMetricListResponse;
import datawave.webservice.query.metric.QueryMetric;
import datawave.webservice.query.metric.QueryMetricListResponse;
import datawave.webservice.query.metric.QueryMetricsDetailListResponse;
import datawave.webservice.query.metric.QueryMetricsSummaryHtmlResponse;
import datawave.webservice.query.metric.QueryMetricsSummaryResponse;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.query.runner.RunningQuery;
import datawave.webservice.query.util.QueryUtil;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.BaseResponse;
import datawave.webservice.result.EventQueryResponseBase;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.deltaspike.core.api.config.ConfigProperty;
import org.apache.deltaspike.core.api.exclude.Exclude;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

@ApplicationScoped
@Exclude(ifProjectStage = DatawaveEmbeddedProjectStageHolder.DatawaveEmbedded.class)
@SuppressWarnings("unused")
public class ShardTableQueryMetricHandler extends BaseQueryMetricHandler<QueryMetric> {
    private static final Logger log = ThreadConfigurableLogger.getLogger(ShardTableQueryMetricHandler.class);
    
    private static final String QUERY_METRICS_LOGIC_NAME = "QueryMetricsQuery";
    protected static final String DEFAULT_SECURITY_MARKING = "PUBLIC";
    
    @Inject
    private AccumuloConnectionFactory connectionFactory;
    
    @Inject
    private QueryLogicFactory queryLogicFactory;
    
    @Inject
    @ConfigProperty(name = "dw.query.metrics.marking")
    protected String markingString;
    
    @Inject
    @ConfigProperty(name = "dw.query.metrics.visibility")
    protected String visibilityString;
    
    @Inject
    private QueryMetricFactory metricFactory;
    
    private Collection<String> connectorAuthorizationCollection = null;
    private String connectorAuthorizations = null;
    private MarkingFunctions markingFunctions = null;
    
    @SuppressWarnings("FieldCanBeLocal")
    private final String JOB_ID = "job_201109071404_1";
    @SuppressWarnings("FieldCanBeLocal")
    private static final String NULL_BYTE = "\0";
    public static final String CONTEXT_WRITER_MAX_CACHE_SIZE = "context.writer.max.cache.size";
    
    // static to share the cache across instances of this class held by QueryExecutorBean, CachedResultsBean, QueryMetricsEnrichmentInterceptor, etc
    @SuppressWarnings("unchecked")
    private static Map metricsCache = Collections.synchronizedMap(new LRUMap(5000));
    
    private final Configuration conf = new Configuration();
    private final StatusReporter reporter = new MockStatusReporter();
    private final AtomicBoolean tablesChecked = new AtomicBoolean(false);
    private AccumuloRecordWriter recordWriter = null;
    
    private UIDBuilder<UID> uidBuilder = UID.builder();
    
    public ShardTableQueryMetricHandler() {
        URL queryMetricsUrl = Thread.currentThread().getContextClassLoader().getResource("datawave/query/QueryMetrics.xml");
        Preconditions.checkNotNull(queryMetricsUrl);
        conf.addResource(queryMetricsUrl);
        
        // encode the password because that's how the AccumuloRecordWriter
        String accumuloPassword = conf.get("AccumuloRecordWriter.password");
        byte[] encodedAccumuloPassword = Base64.encodeBase64(accumuloPassword.getBytes());
        conf.set("AccumuloRecordWriter.password", new String(encodedAccumuloPassword));
        markingFunctions = MarkingFunctions.Factory.createMarkingFunctions();
    }
    
    @PostConstruct
    private void initialize() {
        Connector connector = null;
        
        try {
            connector = connectionFactory.getConnection(Priority.ADMIN, new HashMap<>());
            connectorAuthorizations = connector.securityOperations().getUserAuthorizations(connector.whoami()).toString();
            connectorAuthorizationCollection = Lists.newArrayList(StringUtils.split(connectorAuthorizations, ","));
            reload();
            
            if (tablesChecked.compareAndSet(false, true))
                verifyTables();
        } catch (Exception e) {
            log.error("Error setting connection factory", e);
        } finally {
            if (connector != null) {
                try {
                    connectionFactory.returnConnection(connector);
                } catch (Exception e) {
                    log.error("Error returning connection to connection factory", e);
                }
            }
        }
    }
    
    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        this.recordWriter.close(null);
    }
    
    @Override
    public void flush() throws Exception {
        this.recordWriter.flush();
    }
    
    private void verifyTables() {
        Connector connector = null;
        
        try {
            connector = this.connectionFactory.getConnection(Priority.ADMIN, new HashMap<>());
            AbstractColumnBasedHandler<Key> handler = new ContentQueryMetricsHandler<>();
            createAndConfigureTablesIfNecessary(handler.getTableNames(conf), connector.tableOperations(), conf);
        } catch (Exception e) {
            log.error("Error verifying table configuration", e);
        } finally {
            if (null != connector) {
                try {
                    this.connectionFactory.returnConnection(connector);
                } catch (Exception e) {
                    log.error("Error returning connection to connection factory");
                }
            }
        }
    }
    
    private void writeMetrics(QueryMetric updatedQueryMetric, List<QueryMetric> storedQueryMetrics, Date lastUpdated, boolean delete) throws Exception {
        LiveContextWriter contextWriter = null;
        
        MapContext<Text,RawRecordContainer,Text,Mutation> context = null;
        
        try {
            contextWriter = new LiveContextWriter();
            contextWriter.setup(conf, false);
            
            TaskAttemptID taskId = new TaskAttemptID(new TaskID(new JobID(JOB_ID, 1), TaskType.MAP, 1), 1);
            context = new MapContextImpl<>(conf, taskId, null, recordWriter, null, reporter, null);
            
            for (QueryMetric storedQueryMetric : storedQueryMetrics) {
                AbstractColumnBasedHandler<Key> handler = new ContentQueryMetricsHandler<>();
                handler.setup(context);
                
                Multimap<BulkIngestKey,Value> r = getEntries(handler, updatedQueryMetric, storedQueryMetric, lastUpdated, delete);
                
                try {
                    if (r != null) {
                        contextWriter.write(r, context);
                    }
                    
                    if (handler.getMetadata() != null) {
                        contextWriter.write(handler.getMetadata().getBulkMetadata(), context);
                    }
                } finally {
                    contextWriter.commit(context);
                }
            }
        } finally {
            if (contextWriter != null && context != null) {
                contextWriter.cleanup(context);
            }
        }
    }
    
    public Map<String,String> getEventFields(BaseQueryMetric queryMetric) {
        // ignore duplicates as none are expected
        Map<String,String> eventFields = new HashMap<>();
        ContentQueryMetricsIngestHelper ingestHelper = new ContentQueryMetricsIngestHelper(false);
        ingestHelper.setup(conf);
        Multimap<String,NormalizedContentInterface> fieldsToWrite = ingestHelper.getEventFieldsToWrite(queryMetric);
        for (Entry<String,NormalizedContentInterface> entry : fieldsToWrite.entries()) {
            eventFields.put(entry.getKey(), entry.getValue().getEventFieldValue());
        }
        return eventFields;
    }
    
    private Multimap<BulkIngestKey,Value> getEntries(AbstractColumnBasedHandler<Key> handler, QueryMetric updatedQueryMetric, QueryMetric storedQueryMetric,
                    Date lastUpdated, boolean delete) {
        Type type = TypeRegistry.getType("querymetrics");
        ContentQueryMetricsIngestHelper ingestHelper = new ContentQueryMetricsIngestHelper(delete);
        
        ingestHelper.setup(conf);
        
        RawRecordContainerImpl event = new RawRecordContainerImpl();
        event.setConf(this.conf);
        event.setDataType(type);
        event.setDate(storedQueryMetric.getCreateDate().getTime());
        // get security markings from metric, otherwise default to PUBLIC
        Map<String,String> markings = updatedQueryMetric.getMarkings();
        if (markingFunctions == null || markings == null || markings.isEmpty()) {
            event.setVisibility(new ColumnVisibility(DEFAULT_SECURITY_MARKING));
        } else {
            try {
                event.setVisibility(this.markingFunctions.translateToColumnVisibility(markings));
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                event.setVisibility(new ColumnVisibility(DEFAULT_SECURITY_MARKING));
            }
        }
        event.setAuxData(storedQueryMetric);
        event.setRawRecordNumber(1000L);
        event.addAltId(storedQueryMetric.getQueryId());
        
        event.setId(uidBuilder.newId(storedQueryMetric.getQueryId().getBytes(), (Date) null));
        
        final Multimap<String,NormalizedContentInterface> fields;
        
        if (delete) {
            fields = ingestHelper.getEventFieldsToDelete(updatedQueryMetric, storedQueryMetric);
        } else {
            fields = ingestHelper.getEventFieldsToWrite(updatedQueryMetric);
        }
        
        Key key = new Key();
        
        if (handler.getMetadata() != null) {
            handler.getMetadata().addEventWithoutLoadDates(ingestHelper, event, fields);
        }
        
        String eventTable = handler.getShardTableName().toString();
        String indexTable = handler.getShardIndexTableName().toString();
        String reverseIndexTable = handler.getShardReverseIndexTableName().toString();
        int fieldSizeThreshold = ingestHelper.getFieldSizeThreshold();
        Multimap<BulkIngestKey,Value> r = handler.processBulk(key, event, fields, reporter);
        List<BulkIngestKey> keysToRemove = new ArrayList<>();
        Map<String,BulkIngestKey> tfFields = new HashMap<>();
        
        // if an event has more than two entries for a given field, only keep the longest
        for (Entry<BulkIngestKey,Collection<Value>> entry : r.asMap().entrySet()) {
            String table = entry.getKey().getTableName().toString();
            BulkIngestKey bulkIngestKey = entry.getKey();
            Key currentKey = bulkIngestKey.getKey();
            
            if (table.equals(indexTable) || table.equals(reverseIndexTable)) {
                String value = currentKey.getRow().toString();
                if (value.length() > fieldSizeThreshold) {
                    keysToRemove.add(bulkIngestKey);
                }
            }
        }
        
        // remove any keys from the index or reverseIndex where the value size exceeds the fieldSizeThreshold
        for (BulkIngestKey b : keysToRemove) {
            r.removeAll(b);
        }
        
        // replace the longest of the keys from fields that get parsed as content
        for (Entry<String,BulkIngestKey> l : tfFields.entrySet()) {
            r.put(l.getValue(), new Value(new byte[0]));
        }
        
        for (Entry<BulkIngestKey,Collection<Value>> entry : r.asMap().entrySet()) {
            if (delete) {
                entry.getKey().getKey().setTimestamp(lastUpdated.getTime());
            } else {
                // this will ensure that the QueryMetrics can be found within second precision in most cases
                entry.getKey().getKey().setTimestamp(storedQueryMetric.getCreateDate().getTime() + storedQueryMetric.getNumUpdates());
            }
            entry.getKey().getKey().setDeleted(delete);
        }
        
        return r;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void updateMetric(QueryMetric updatedQueryMetric, DatawavePrincipal datawavePrincipal) throws Exception {
        Date lastUpdated = updatedQueryMetric.getLastUpdated();
        
        try {
            enableLogs(false);
            String sid = updatedQueryMetric.getUser();
            if (sid == null) {
                sid = datawavePrincipal.getShortName();
            }
            
            // find and remove previous entries
            BaseQueryMetricListResponse response = new QueryMetricListResponse();
            Date end = new Date();
            Date begin = DateUtils.setYears(end, 2000);
            
            // user's DatawavePrincipal must have the Administrator role to use the Metrics query logic
            QueryMetric cachedQueryMetric;
            QueryMetric newCachedQueryMetric;
            synchronized (ShardTableQueryMetricHandler.class) {
                cachedQueryMetric = (QueryMetric) metricsCache.get(updatedQueryMetric.getQueryId());
                // duplicate updatedQueryMetric because we're counting on the cache to be a snapshot of the QueryMetric
                // so that we can retrieve it next update call to create the delete Mutations for the values written to Accumulo
                Map<Long,PageMetric> storedPageMetricMap = new TreeMap<>();
                if (cachedQueryMetric != null) {
                    List<PageMetric> cachedPageMetrics = cachedQueryMetric.getPageTimes();
                    if (cachedPageMetrics != null) {
                        for (PageMetric p : cachedPageMetrics) {
                            storedPageMetricMap.put(p.getPageNumber(), p);
                        }
                    }
                }
                // combine all of the page metrics from the cached metric and the updated metric
                for (PageMetric p : updatedQueryMetric.getPageTimes()) {
                    storedPageMetricMap.put(p.getPageNumber(), p);
                }
                newCachedQueryMetric = (QueryMetric) updatedQueryMetric.duplicate();
                ArrayList<PageMetric> newPageMetrics = new ArrayList<>();
                newPageMetrics.addAll(storedPageMetricMap.values());
                newCachedQueryMetric.setPageTimes(newPageMetrics);
                metricsCache.put(updatedQueryMetric.getQueryId(), newCachedQueryMetric);
            }
            
            List<QueryMetric> queryMetrics = new ArrayList<>();
            
            if (cachedQueryMetric == null) {
                // if numPages > 0 or Lifecycle > DEFINED, then we should have a metric cached already
                // if we don't, then query for the current stored metric
                if (updatedQueryMetric.getNumPages() > 0 || updatedQueryMetric.getLifecycle().compareTo(Lifecycle.DEFINED) > 0) {
                    QueryImpl query = new QueryImpl();
                    query.setBeginDate(begin);
                    query.setEndDate(end);
                    query.setQueryLogicName(QUERY_METRICS_LOGIC_NAME);
                    query.setQuery("QUERY_ID == '" + updatedQueryMetric.getQueryId() + "'");
                    query.setQueryName(QUERY_METRICS_LOGIC_NAME);
                    query.setColumnVisibility(visibilityString);
                    query.setQueryAuthorizations(connectorAuthorizations);
                    query.setUserDN(sid);
                    query.setExpirationDate(DateUtils.addDays(new Date(), 1));
                    query.setPagesize(1000);
                    query.setId(UUID.randomUUID());
                    query.setParameters(ImmutableMap.of(QueryOptions.INCLUDE_GROUPING_CONTEXT, "true"));
                    queryMetrics = getQueryMetrics(response, query, datawavePrincipal);
                }
            } else {
                queryMetrics = Collections.singletonList(cachedQueryMetric);
            }
            
            if (!queryMetrics.isEmpty()) {
                writeMetrics(updatedQueryMetric, queryMetrics, lastUpdated, true);
            }
            
            long nextUpdateNumber = 0;
            
            for (BaseQueryMetric m : queryMetrics) {
                if ((m.getNumUpdates() + 1) > nextUpdateNumber) {
                    nextUpdateNumber = m.getNumUpdates() + 1;
                }
            }
            
            updatedQueryMetric.setNumUpdates(nextUpdateNumber);
            
            synchronized (ShardTableQueryMetricHandler.class) {
                newCachedQueryMetric.setNumUpdates(nextUpdateNumber);
                metricsCache.put(updatedQueryMetric.getQueryId(), newCachedQueryMetric);
            }
            
            // write new entry
            writeMetrics(updatedQueryMetric, Collections.singletonList(updatedQueryMetric), lastUpdated, false);
        } finally {
            enableLogs(true);
        }
    }
    
    private List<QueryMetric> getQueryMetrics(BaseResponse response, Query query, DatawavePrincipal datawavePrincipal) {
        List<QueryMetric> queryMetrics = new ArrayList<>();
        RunningQuery runningQuery = null;
        Connector connector = null;
        
        try {
            Map<String,String> trackingMap = this.connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            connector = this.connectionFactory.getConnection(Priority.ADMIN, trackingMap);
            QueryLogic<?> queryLogic = queryLogicFactory.getQueryLogic(query.getQueryLogicName(), datawavePrincipal);
            if (queryLogic instanceof QueryMetricQueryLogic) {
                ((QueryMetricQueryLogic) queryLogic).setRolesSets(datawavePrincipal.getPrimaryUser().getRoles());
            }
            runningQuery = new RunningQuery(null, connector, Priority.ADMIN, queryLogic, query, query.getQueryAuthorizations(), datawavePrincipal,
                            metricFactory);
            
            boolean done = false;
            List<Object> objectList = new ArrayList<>();
            
            while (!done) {
                ResultsPage resultsPage = runningQuery.next();
                
                if (!resultsPage.getResults().isEmpty()) {
                    objectList.addAll(resultsPage.getResults());
                } else {
                    done = true;
                }
            }
            
            BaseQueryResponse queryResponse = queryLogic.getTransformer(query).createResponse(new ResultsPage(objectList));
            List<QueryExceptionType> exceptions = queryResponse.getExceptions();
            
            if (queryResponse.getExceptions() != null && !queryResponse.getExceptions().isEmpty()) {
                if (response != null) {
                    response.setExceptions(new LinkedList<>(exceptions));
                    response.setHasResults(false);
                }
            }
            
            if (!(queryResponse instanceof EventQueryResponseBase)) {
                if (response != null) {
                    response.addException(new QueryException("incompatible response")); // TODO: Should this be an IllegalStateException?
                    response.setHasResults(false);
                }
            }
            
            EventQueryResponseBase eventQueryResponse = (EventQueryResponseBase) queryResponse;
            List<EventBase> eventList = eventQueryResponse.getEvents();
            
            for (EventBase<?,?> event : eventList) {
                QueryMetric metric = toMetric(event);
                queryMetrics.add(metric);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            if (response != null) {
                response.addExceptions(new QueryException(e).getQueryExceptionsInStack());
            }
        } finally {
            if (null != this.connectionFactory) {
                if (null != runningQuery && null != connector) {
                    try {
                        runningQuery.closeConnection(this.connectionFactory);
                    } catch (Exception e) {
                        log.warn("Could not return connector to factory", e);
                    }
                } else if (null != connector) {
                    try {
                        this.connectionFactory.returnConnection(connector);
                    } catch (Exception e) {
                        log.warn("Could not return connector to factory", e);
                    }
                }
            }
        }
        
        return queryMetrics;
    }
    
    @Override
    public QueryMetricListResponse query(String user, String queryId, DatawavePrincipal datawavePrincipal) {
        QueryMetricsDetailListResponse response = new QueryMetricsDetailListResponse();
        
        try {
            enableLogs(false);
            
            Collection<? extends Collection<String>> authorizations = datawavePrincipal.getAuthorizations();
            Date end = new Date();
            Date begin = DateUtils.setYears(end, 2000);
            
            QueryImpl query = new QueryImpl();
            query.setBeginDate(begin);
            query.setEndDate(end);
            query.setQueryLogicName(QUERY_METRICS_LOGIC_NAME);
            // QueryMetricQueryLogic now enforces that you must be a QueryMetricsAdministrator to query metrics that do not belong to you
            query.setQuery("QUERY_ID == '" + queryId + "'");
            query.setQueryName(QUERY_METRICS_LOGIC_NAME);
            query.setColumnVisibility(visibilityString);
            query.setQueryAuthorizations(AuthorizationsUtil.buildAuthorizationString(authorizations));
            query.setExpirationDate(DateUtils.addDays(new Date(), 1));
            query.setPagesize(1000);
            query.setUserDN(datawavePrincipal.getShortName());
            query.setOwner(datawavePrincipal.getShortName());
            query.setId(UUID.randomUUID());
            query.setParameters(ImmutableMap.of(QueryOptions.INCLUDE_GROUPING_CONTEXT, "true"));
            List<QueryMetric> queryMetrics = getQueryMetrics(response, query, datawavePrincipal);
            
            response.setResult(queryMetrics);
            
            response.setGeoQuery(queryMetrics.stream().anyMatch(SimpleQueryGeometryHandler::isGeoQuery));
        } finally {
            enableLogs(true);
        }
        
        return response;
    }
    
    public QueryMetric toMetric(datawave.webservice.query.result.event.EventBase event) {
        SimpleDateFormat sdf_date_time1 = new SimpleDateFormat("yyyyMMdd HHmmss");
        SimpleDateFormat sdf_date_time2 = new SimpleDateFormat("yyyyMMdd HHmmss");
        SimpleDateFormat sdf_date_time3 = new SimpleDateFormat("yyyyMMdd");
        
        List<String> excludedFields = Arrays.asList("ELAPSED_TIME", "RECORD_ID", "NUM_PAGES", "NUM_RESULTS");
        
        try {
            QueryMetric m = new QueryMetric();
            List<FieldBase> field = event.getFields();
            m.setMarkings(event.getMarkings());
            TreeMap<Long,PageMetric> pageMetrics = Maps.newTreeMap();
            
            boolean createDateSet = false;
            for (FieldBase f : field) {
                String fieldName = f.getName();
                String fieldValue = f.getValueString();
                if (!excludedFields.contains(fieldName)) {
                    if (fieldName.equals("USER")) {
                        m.setUser(fieldValue);
                    } else if (fieldName.equals("USER_DN")) {
                        m.setUserDN(fieldValue);
                    } else if (fieldName.equals("QUERY_ID")) {
                        m.setQueryId(fieldValue);
                    } else if (fieldName.equals("CREATE_DATE")) {
                        try {
                            Date d = sdf_date_time2.parse(fieldValue);
                            m.setCreateDate(d);
                            createDateSet = true;
                        } catch (Exception e) {
                            log.error(e.getMessage());
                        }
                    } else if (fieldName.equals("QUERY")) {
                        m.setQuery(fieldValue);
                    } else if (fieldName.equals("PLAN")) {
                        m.setPlan(fieldValue);
                    } else if (fieldName.equals("QUERY_LOGIC")) {
                        m.setQueryLogic(fieldValue);
                    } else if (fieldName.equals("QUERY_ID")) {
                        m.setQueryId(fieldValue);
                    } else if (fieldName.equals("BEGIN_DATE")) {
                        try {
                            Date d = sdf_date_time1.parse(fieldValue);
                            m.setBeginDate(d);
                        } catch (Exception e) {
                            log.error(e.getMessage());
                        }
                    } else if (fieldName.equals("END_DATE")) {
                        try {
                            Date d = sdf_date_time1.parse(fieldValue);
                            m.setEndDate(d);
                        } catch (Exception e) {
                            log.error(e.getMessage());
                        }
                    } else if (fieldName.equals("HOST")) {
                        m.setHost(fieldValue);
                    } else if (fieldName.equals("PROXY_SERVERS")) {
                        m.setProxyServers(Arrays.asList(StringUtils.split(fieldValue, ",")));
                    } else if (fieldName.equals("AUTHORIZATIONS")) {
                        m.setQueryAuthorizations(fieldValue);
                    } else if (fieldName.equals("QUERY_TYPE")) {
                        m.setQueryType(fieldValue);
                    } else if (fieldName.equals("LIFECYCLE")) {
                        m.setLifecycle(Lifecycle.valueOf(fieldValue));
                    } else if (fieldName.equals("ERROR_CODE")) {
                        m.setErrorCode(fieldValue);
                    } else if (fieldName.equals("ERROR_MESSAGE")) {
                        m.setErrorMessage(fieldValue);
                    } else if (fieldName.equals("SETUP_TIME")) {
                        m.setSetupTime(Long.parseLong(fieldValue));
                    } else if (fieldName.equals("CREATE_CALL_TIME")) {
                        m.setCreateCallTime(Long.parseLong(fieldValue));
                    } else if (fieldName.startsWith("PAGE_METRICS")) {
                        int index = fieldName.indexOf(".");
                        if (-1 == index) {
                            log.error("Could not parse field name to extract repetition count: " + fieldName);
                        } else {
                            Long pageNum = Long.parseLong(fieldName.substring(index + 1));
                            PageMetric pageMetric = PageMetric.parse(fieldValue);
                            if (pageMetric != null) {
                                pageMetric.setPageNumber(pageNum);
                                pageMetrics.put(pageNum, pageMetric);
                            }
                        }
                    } else if (fieldName.equals("POSITIVE_SELECTORS")) {
                        List<String> positiveSelectors = m.getPositiveSelectors();
                        if (positiveSelectors == null) {
                            positiveSelectors = new ArrayList<>();
                        }
                        positiveSelectors.add(fieldValue);
                        m.setPositiveSelectors(positiveSelectors);
                    } else if (fieldName.equals("NEGATIVE_SELECTORS")) {
                        List<String> negativeSelectors = m.getNegativeSelectors();
                        if (negativeSelectors == null) {
                            negativeSelectors = new ArrayList<>();
                        }
                        negativeSelectors.add(fieldValue);
                        m.setNegativeSelectors(negativeSelectors);
                    } else if (fieldName.equals("LAST_UPDATED")) {
                        try {
                            Date d = sdf_date_time2.parse(fieldValue);
                            m.setLastUpdated(d);
                        } catch (Exception e) {
                            log.error(e.getMessage());
                        }
                    } else if (fieldName.equals("NUM_UPDATES")) {
                        try {
                            long numUpdates = Long.parseLong(fieldValue);
                            m.setNumUpdates(numUpdates);
                        } catch (Exception e) {
                            log.error(e.getMessage());
                        }
                    } else if (fieldName.equals("QUERY_NAME")) {
                        m.setQueryName(fieldValue);
                    } else if (fieldName.equals("PARAMETERS")) {
                        if (fieldValue != null) {
                            try {
                                Set<Parameter> parameters = QueryUtil.parseParameters(fieldValue);
                                m.setParameters(parameters);
                                
                            } catch (Exception e) {
                                log.debug(e.getMessage());
                            }
                        }
                    } else if (fieldName.equals("SOURCE_COUNT")) {
                        m.setSourceCount(Long.parseLong(fieldValue));
                    } else if (fieldName.equals("NEXT_COUNT")) {
                        m.setNextCount(Long.parseLong(fieldValue));
                    } else if (fieldName.equals("SEEK_COUNT")) {
                        m.setSeekCount(Long.parseLong(fieldValue));
                    } else if (fieldName.equals("YIELD_COUNT")) {
                        m.setYieldCount(Long.parseLong(fieldValue));
                    } else if (fieldName.equals("DOC_RANGES")) {
                        m.setDocRanges(Long.parseLong(fieldValue));
                    } else if (fieldName.equals("FI_RANGES")) {
                        m.setFiRanges(Long.parseLong(fieldValue));
                    } else if (fieldName.equals("VERSION")) {
                        m.setVersion(fieldValue);
                    } else if (fieldName.equals("YIELD_COUNT")) {
                        m.setYieldCount(Long.parseLong(fieldValue));
                    } else if (fieldName.equals("LOGIN_TIME")) {
                        m.setLoginTime(Long.parseLong(fieldValue));
                    } else {
                        log.debug("encountered unanticipated field name: " + fieldName);
                    }
                }
            }
            // if createDate has not been set, try to parse it from the event row
            if (!createDateSet) {
                try {
                    String dateStr = event.getMetadata().getRow().substring(0, 8);
                    m.setCreateDate(sdf_date_time3.parse(dateStr));
                } catch (ParseException e) {
                    
                }
            }
            m.setPageTimes(new ArrayList<>(pageMetrics.values()));
            return m;
        } catch (Exception e) {
            log.warn("Unexpected error creating query metric. Returning null", e);
            return null;
        }
    }
    
    protected void createAndConfigureTablesIfNecessary(String[] tableNames, TableOperations tops, Configuration conf) throws AccumuloSecurityException,
                    AccumuloException, TableNotFoundException {
        for (String table : tableNames) {
            // If the tables don't exist, then create them.
            try {
                if (!tops.exists(table)) {
                    tops.create(table);
                    Map<String,TableConfigHelper> tableConfigs = getTableConfigs(log, conf, tableNames);
                    
                    TableConfigHelper tableHelper = tableConfigs.get(table);
                    
                    if (tableHelper != null) {
                        tableHelper.configure(tops);
                    } else {
                        log.info("No configuration supplied for table: " + table);
                    }
                }
            } catch (TableExistsException te) {
                // in this case, somebody else must have created the table after our existence check
                log.debug("Tried to create " + table + " but somebody beat us to the punch");
            }
        }
    }
    
    @SuppressWarnings("unchecked")
    private Map<String,TableConfigHelper> getTableConfigs(Logger log, Configuration conf, String[] tableNames) {
        Map<String,TableConfigHelper> helperMap = new HashMap<>(tableNames.length);
        
        for (String table : tableNames) {
            String prop = table + TableConfigHelper.TABLE_CONFIG_CLASS_SUFFIX;
            String className = conf.get(prop, null);
            TableConfigHelper tableHelper = null;
            
            if (className != null) {
                try {
                    Class<? extends TableConfigHelper> tableHelperClass = (Class<? extends TableConfigHelper>) Class.forName(className.trim());
                    tableHelper = tableHelperClass.newInstance();
                    
                    if (tableHelper != null)
                        tableHelper.setup(table, conf, log);
                } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
                    throw new IllegalArgumentException(e);
                }
            }
            
            helperMap.put(table, tableHelper);
        }
        
        return helperMap;
    }
    
    private void enableLogs(boolean enable) {
        if (enable) {
            ThreadConfigurableLogger.clearThreadLevels();
        } else {
            // All loggers that are encountered in the call chain during metrics calls should be included here.
            // If you need to add a logger name here, you also need to change the Logger declaration where that Logger is instantiated
            // Change:
            // Logger log = Logger.getLogger(MyClass.class);
            // to
            // Logger log = ThreadConfigurableLogger.getLogger(MyClass.class);
            
            ThreadConfigurableLogger.setLevelForThread("datawave.query.index.lookup.RangeStream", Level.ERROR);
            ThreadConfigurableLogger.setLevelForThread("datawave.query.metrics.ShardTableQueryMetricHandler", Level.ERROR);
            ThreadConfigurableLogger.setLevelForThread("datawave.query.planner.DefaultQueryPlanner", Level.ERROR);
            ThreadConfigurableLogger.setLevelForThread("datawave.query.planner.ThreadedRangeBundlerIterator", Level.ERROR);
            ThreadConfigurableLogger.setLevelForThread("datawave.query.scheduler.SequentialScheduler", Level.ERROR);
            ThreadConfigurableLogger.setLevelForThread("datawave.query.tables.ShardQueryLogic", Level.ERROR);
            ThreadConfigurableLogger.setLevelForThread("datawave.query.metrics.ShardTableQueryMetricHandler", Level.ERROR);
            ThreadConfigurableLogger.setLevelForThread("datawave.query.jexl.visitors.QueryModelVisitor", Level.ERROR);
            ThreadConfigurableLogger.setLevelForThread("datawave.query.jexl.visitors.ExpandMultiNormalizedTerms", Level.ERROR);
            ThreadConfigurableLogger.setLevelForThread("datawave.query.jexl.lookups.LookupBoundedRangeForTerms", Level.ERROR);
            ThreadConfigurableLogger.setLevelForThread("datawave.query.jexl.visitors.RangeConjunctionRebuildingVisitor", Level.ERROR);
            
            ThreadConfigurableLogger.setLevelForThread("datawave.ingest.data.TypeRegistry", Level.ERROR);
            ThreadConfigurableLogger.setLevelForThread("datawave.ingest.data.config.ingest.BaseIngestHelper", Level.ERROR);
            ThreadConfigurableLogger.setLevelForThread("datawave.ingest.mapreduce.handler.shard.AbstractColumnBasedHandler", Level.ERROR);
            ThreadConfigurableLogger.setLevelForThread("datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler", Level.ERROR);
            ThreadConfigurableLogger.setLevelForThread("datawave.ingest.util.RegionTimer", Level.ERROR);
            ThreadConfigurableLogger.setLevelForThread("datawave.ingest.data.Event", Level.OFF);
        }
    }
    
    @Override
    public void reload() {
        try {
            if (this.recordWriter != null) {
                // don't try to flush the mtbw (close). If recordWriter != null then this method is being called
                // because of an Exception and the metrics have been saved off to be added to the new recordWriter.
                this.recordWriter.returnConnector();
            }
            recordWriter = new AccumuloRecordWriter(this.connectionFactory, conf);
        } catch (AccumuloException | AccumuloSecurityException | IOException e) {
            log.error(e.getMessage(), e);
        }
    }
    
    @Override
    public QueryMetricsSummaryResponse getTotalQueriesSummaryCounts(Date begin, Date end, DatawavePrincipal datawavePrincipal) {
        return getQueryMetricsSummary(begin, end, false, datawavePrincipal, new QueryMetricsSummaryResponse());
    }
    
    @Override
    public QueryMetricsSummaryHtmlResponse getTotalQueriesSummary(Date begin, Date end, DatawavePrincipal datawavePrincipal) {
        return (QueryMetricsSummaryHtmlResponse) getQueryMetricsSummary(begin, end, false, datawavePrincipal, new QueryMetricsSummaryHtmlResponse());
    }
    
    @Override
    public QueryMetricsSummaryHtmlResponse getUserQueriesSummary(Date begin, Date end, DatawavePrincipal datawavePrincipal) {
        return (QueryMetricsSummaryHtmlResponse) getQueryMetricsSummary(begin, end, true, datawavePrincipal, new QueryMetricsSummaryHtmlResponse());
    }
    
    public QueryMetricsSummaryResponse getQueryMetricsSummary(Date begin, Date end, boolean onlyCurrentUser, DatawavePrincipal datawavePrincipal,
                    QueryMetricsSummaryResponse response) {
        
        try {
            enableLogs(false);
            
            Collection<? extends Collection<String>> authorizations = datawavePrincipal.getAuthorizations();
            QueryImpl query = new QueryImpl();
            query.setBeginDate(begin);
            query.setEndDate(end);
            query.setQueryLogicName(QUERY_METRICS_LOGIC_NAME);
            if (onlyCurrentUser) {
                String user = datawavePrincipal.getShortName();
                query.setQuery("USER == '" + user + "'");
            } else {
                query.setQuery("((_Bounded_ = true) && (USER > 'A' && USER < 'ZZZZZZZ'))");
            }
            query.setQueryName(QUERY_METRICS_LOGIC_NAME);
            query.setColumnVisibility(visibilityString);
            query.setQueryAuthorizations(AuthorizationsUtil.buildAuthorizationString(authorizations));
            query.setExpirationDate(DateUtils.addDays(new Date(), 1));
            query.setPagesize(1000);
            query.setUserDN(datawavePrincipal.getShortName());
            query.setId(UUID.randomUUID());
            query.setParameters(ImmutableMap.of(QueryOptions.INCLUDE_GROUPING_CONTEXT, "true"));
            
            List<QueryMetric> queryMetrics = getQueryMetrics(response, query, datawavePrincipal);
            response = processQueryMetricsSummary(queryMetrics, end, response);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        } finally {
            enableLogs(true);
        }
        
        return response;
    }
}
