package datawave.microservice.querymetric.handler;

import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.NamespaceExistsException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;

import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import datawave.core.common.connection.AccumuloClientPool;
import datawave.core.query.util.QueryUtil;
import datawave.data.hash.UID;
import datawave.data.hash.UIDBuilder;
import datawave.ingest.config.RawRecordContainerImpl;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.AbstractContentIngestHelper;
import datawave.ingest.mapreduce.handler.shard.AbstractColumnBasedHandler;
import datawave.ingest.mapreduce.handler.tokenize.ContentIndexingColumnBasedHandler;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.table.config.TableConfigHelper;
import datawave.marking.MarkingFunctions;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.BaseQueryMetric.Lifecycle;
import datawave.microservice.querymetric.BaseQueryMetric.PageMetric;
import datawave.microservice.querymetric.BaseQueryMetric.Prediction;
import datawave.microservice.querymetric.QueryMetricFactory;
import datawave.microservice.querymetric.QueryMetricType;
import datawave.microservice.querymetric.QueryMetricsSummaryResponse;
import datawave.microservice.querymetric.config.QueryMetricHandlerProperties;
import datawave.microservice.querymetric.factory.QueryMetricQueryLogicFactory;
import datawave.microservice.security.util.DnUtils;
import datawave.query.QueryParameters;
import datawave.query.iterator.QueryOptions;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import datawave.security.authorization.DatawaveUser;
import datawave.security.util.WSAuthorizationsUtil;
import datawave.webservice.query.exception.QueryExceptionType;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.EventQueryResponseBase;

public abstract class ShardTableQueryMetricHandler<T extends BaseQueryMetric> extends BaseQueryMetricHandler<T> {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final org.apache.log4j.Logger setupLogger = org.apache.log4j.Logger.getLogger(getClass());
    
    protected String clientAuthorizations;
    
    protected AccumuloClientPool accumuloClientPool;
    protected QueryMetricHandlerProperties queryMetricHandlerProperties;
    
    @SuppressWarnings("FieldCanBeLocal")
    protected final String JOB_ID = "job_201109071404_1";
    protected static final String BLACKLISTED_FIELDS_DEPRECATED = "blacklisted.fields";
    
    protected final Configuration conf = new Configuration();
    protected final StatusReporter reporter = new MockStatusReporter();
    protected final AtomicBoolean tablesChecked = new AtomicBoolean(false);
    protected AccumuloRecordWriter recordWriter = null;
    protected QueryMetricQueryLogicFactory logicFactory;
    protected QueryMetricFactory metricFactory;
    protected UIDBuilder<UID> uidBuilder = UID.builder();
    protected QueryMetricCombiner queryMetricCombiner;
    protected MarkingFunctions markingFunctions;
    protected DnUtils dnUtils;
    // this lock is necessary for when there is an error condition and the accumuloRecordWriter needs to be replaced
    protected ReentrantReadWriteLock accumuloRecordWriterLock = new ReentrantReadWriteLock();
    
    public ShardTableQueryMetricHandler(QueryMetricHandlerProperties queryMetricHandlerProperties,
                    @Qualifier("warehouse") AccumuloClientPool accumuloClientPool, QueryMetricQueryLogicFactory logicFactory, QueryMetricFactory metricFactory,
                    MarkingFunctions markingFunctions, QueryMetricCombiner queryMetricCombiner, LuceneToJexlQueryParser luceneToJexlQueryParser,
                    DnUtils dnUtils) {
        super(luceneToJexlQueryParser);
        this.queryMetricHandlerProperties = queryMetricHandlerProperties;
        this.logicFactory = logicFactory;
        this.metricFactory = metricFactory;
        this.markingFunctions = markingFunctions;
        this.dnUtils = dnUtils;
        this.accumuloClientPool = accumuloClientPool;
        this.queryMetricCombiner = queryMetricCombiner;
        
        queryMetricHandlerProperties.getProperties().entrySet().forEach(e -> conf.set(e.getKey(), e.getValue()));
        
        AccumuloClient accumuloClient = null;
        try {
            log.info("creating connector with username:" + queryMetricHandlerProperties.getUsername());
            Map<String,String> trackingMap = AccumuloClientTracking.getTrackingMap(Thread.currentThread().getStackTrace());
            accumuloClient = accumuloClientPool.borrowObject(trackingMap);
            this.clientAuthorizations = accumuloClient.securityOperations().getUserAuthorizations(accumuloClient.whoami()).toString();
            reload();
            
            if (this.tablesChecked.compareAndSet(false, true)) {
                verifyTables();
            }
            initializeMetadata();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            if (accumuloClient != null) {
                this.accumuloClientPool.returnObject(accumuloClient);
            }
        }
    }
    
    protected void initializeMetadata() throws Exception {
        T metric = (T) metricFactory.createMetric(true);
        populateFieldsForMetadata(metric);
        writeMetadata(metric);
        flush();
    }
    
    protected void populateFieldsForMetadata(T metric) {
        // these values are not written and are only being set to trigger the ingest
        // framework to write the entries for these fields into the metadata table
        Date d = new Date();
        metric.setQueryId(UUID.randomUUID().toString());
        metric.setQuery("QUERY");
        metric.setSetupTime(1);
        metric.setCreateCallTime(1);
        metric.setBeginDate(d);
        metric.setEndDate(d);
        metric.setPlan("PLAN");
        metric.setError(new RuntimeException());
        metric.setErrorMessage("ERROR");
        metric.setErrorCode("ERROR");
        metric.setQueryAuthorizations("AUTHS");
        metric.setHost("localhost");
        metric.setLastUpdated(d);
        metric.setLoginTime(1);
        metric.setNegativeSelectors(Collections.singletonList("SELECTOR"));
        metric.setPositiveSelectors(Collections.singletonList("SELECTOR"));
        metric.setParameters(new HashSet<>(Arrays.asList(new QueryImpl.Parameter())));
        metric.setPredictions(new HashSet<>(Arrays.asList(new Prediction())));
        metric.setProxyServers(Collections.singletonList("SERVER"));
        metric.setQueryLogic("LOGIC");
        metric.setQueryName("QUERY");
        metric.setQueryType("TYPE");
        metric.setUser("USER");
        metric.setUserDN("USERDN");
        metric.addPageTime(100, 100, 1000, 1000);
    }
    
    public void shutdown() throws Exception {
        if (this.recordWriter != null) {
            this.accumuloRecordWriterLock.writeLock().lock();
            try {
                this.recordWriter.close(null);
                this.recordWriter = null;
            } finally {
                this.accumuloRecordWriterLock.writeLock().unlock();
            }
        }
    }
    
    @Override
    public void flush() throws Exception {
        if (this.recordWriter != null) {
            this.accumuloRecordWriterLock.readLock().lock();
            try {
                this.recordWriter.flush();
            } finally {
                this.accumuloRecordWriterLock.readLock().unlock();
            }
        }
    }
    
    public void verifyTables() {
        AccumuloClient accumuloClient = null;
        try {
            AbstractColumnBasedHandler<Key> handler = new ContentIndexingColumnBasedHandler() {
                @Override
                public AbstractContentIngestHelper getContentIndexingDataTypeHelper() {
                    return getQueryMetricsIngestHelper(false, Collections.EMPTY_LIST);
                }
            };
            Map<String,String> trackingMap = AccumuloClientTracking.getTrackingMap(Thread.currentThread().getStackTrace());
            accumuloClient = accumuloClientPool.borrowObject(trackingMap);
            createAndConfigureTablesIfNecessary(handler.getTableNames(conf), accumuloClient, conf);
        } catch (Exception e) {
            log.error("Error verifying table configuration", e);
        } finally {
            if (accumuloClient != null) {
                this.accumuloClientPool.returnObject(accumuloClient);
            }
        }
    }
    
    private void writeMetadata(ContentIndexingColumnBasedHandler handler) throws Exception {
        if (handler.getMetadata() != null) {
            for (Entry<BulkIngestKey,Value> e : handler.getMetadata().getBulkMetadata().entries()) {
                recordWriter.write(e.getKey().getTableName(), getMutation(e.getKey().getKey(), e.getValue()));
            }
        }
    }
    
    private void writeMetric(T updated, T stored, long timestamp, boolean delete, ContentIndexingColumnBasedHandler handler) throws Exception {
        Multimap<BulkIngestKey,Value> r = getEntries(handler, updated, stored, timestamp);
        if (r != null) {
            for (Entry<BulkIngestKey,Value> e : r.entries()) {
                recordWriter.write(e.getKey().getTableName(), getMutation(e.getKey().getKey(), e.getValue()));
            }
        }
        if (!delete) {
            writeMetadata(handler);
        }
    }
    
    public void writeMetric(T updatedQueryMetric, List<T> storedQueryMetrics, long timestamp, boolean delete) throws Exception {
        writeMetric(updatedQueryMetric, storedQueryMetrics, timestamp, delete, Collections.EMPTY_LIST);
    }
    
    public void writeMetadata(T metric) throws Exception {
        try {
            TaskAttemptID taskId = new TaskAttemptID(new TaskID(new JobID(JOB_ID, 1), TaskType.MAP, 1), 1);
            this.accumuloRecordWriterLock.readLock().lock();
            
            try {
                MapContext<Text,RawRecordContainer,Text,Mutation> context = new MapContextImpl<>(conf, taskId, null, this.recordWriter, null, reporter, null);
                ContentIndexingColumnBasedHandler handler = new ContentIndexingColumnBasedHandler() {
                    @Override
                    public AbstractContentIngestHelper getContentIndexingDataTypeHelper() {
                        return getQueryMetricsIngestHelper(false, Collections.emptyList());
                    }
                };
                handler.setup(context);
                getEntries(handler, metric, null, System.currentTimeMillis());
                writeMetadata(handler);
            } finally {
                this.accumuloRecordWriterLock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            // assume that an error happened with the AccumuloRecordWriter
            // mark recordWriter as unhealthy -- the first thread to get the writeLock in
            // reload will create a new one that will be marked healthy
            this.recordWriter.setHealthy(false);
            reload();
            // we have no way of knowing if the rejected mutation is this one or a previously
            // written one throw the exception so that the metric will be re-written
            throw e;
        }
    }
    
    public void writeMetric(T updatedQueryMetric, List<T> storedQueryMetrics, long timestamp, boolean delete, Collection<String> ignoredFields)
                    throws Exception {
        try {
            TaskAttemptID taskId = new TaskAttemptID(new TaskID(new JobID(JOB_ID, 1), TaskType.MAP, 1), 1);
            this.accumuloRecordWriterLock.readLock().lock();
            
            try {
                MapContext<Text,RawRecordContainer,Text,Mutation> context = new MapContextImpl<>(conf, taskId, null, this.recordWriter, null, reporter, null);
                ContentIndexingColumnBasedHandler handler = new ContentIndexingColumnBasedHandler() {
                    @Override
                    public AbstractContentIngestHelper getContentIndexingDataTypeHelper() {
                        return getQueryMetricsIngestHelper(delete, ignoredFields);
                    }
                };
                handler.setup(context);
                if (storedQueryMetrics.isEmpty()) {
                    writeMetric(updatedQueryMetric, null, timestamp, delete, handler);
                } else {
                    for (T storedQueryMetric : storedQueryMetrics) {
                        writeMetric(updatedQueryMetric, storedQueryMetric, timestamp, delete, handler);
                    }
                }
            } finally {
                this.accumuloRecordWriterLock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            // assume that an error happened with the AccumuloRecordWriter
            // mark recordWriter as unhealthy -- the first thread to get the writeLock in
            // reload will create a new one that will be marked healthy
            this.recordWriter.setHealthy(false);
            reload();
            // we have no way of knowing if the rejected mutation is this one or a previously
            // written one throw the exception so that the metric will be re-written
            throw e;
        }
    }
    
    private Mutation getMutation(Key key, Value value) {
        Mutation m = new Mutation(key.getRow());
        if (key.isDeleted()) {
            m.putDelete(key.getColumnFamily(), key.getColumnQualifier(), new ColumnVisibility(key.getColumnVisibility()), key.getTimestamp());
        } else {
            m.put(key.getColumnFamily(), key.getColumnQualifier(), new ColumnVisibility(key.getColumnVisibility()), key.getTimestamp(), value);
        }
        return m;
    }
    
    public Map<String,String> getEventFields(BaseQueryMetric queryMetric) {
        // ignore duplicates as none are expected
        Map<String,String> eventFields = new HashMap<>();
        ContentQueryMetricsIngestHelper ingestHelper = getQueryMetricsIngestHelper(false, Collections.EMPTY_LIST);
        ingestHelper.setup(conf);
        Multimap<String,NormalizedContentInterface> fieldsToWrite = ingestHelper.getEventFieldsToWrite(queryMetric, null);
        for (Entry<String,NormalizedContentInterface> entry : fieldsToWrite.entries()) {
            eventFields.put(entry.getKey(), entry.getValue().getEventFieldValue());
        }
        return eventFields;
    }
    
    protected Multimap<BulkIngestKey,Value> getEntries(ContentIndexingColumnBasedHandler handler, T updatedQueryMetric, T storedQueryMetric, long timestamp) {
        Type type = TypeRegistry.getType("querymetrics");
        ContentQueryMetricsIngestHelper ingestHelper = (ContentQueryMetricsIngestHelper) handler.getContentIndexingDataTypeHelper();
        boolean deleteMode = ingestHelper.getDeleteMode();
        ingestHelper.setup(conf);
        
        RawRecordContainerImpl event = new RawRecordContainerImpl();
        event.setConf(this.conf);
        event.setDataType(type);
        event.setDate(updatedQueryMetric.getCreateDate().getTime());
        // get markings from metric, otherwise use the default markings
        Map<String,String> markings = updatedQueryMetric.getMarkings();
        if (markings != null && !markings.isEmpty()) {
            try {
                event.setVisibility(this.markingFunctions.translateToColumnVisibility(updatedQueryMetric.getMarkings()));
            } catch (MarkingFunctions.Exception e) {
                log.error(e.getMessage(), e);
                event.setVisibility(this.queryMetricHandlerProperties.getDefaultMetricVisibility());
            }
        } else {
            event.setVisibility(this.queryMetricHandlerProperties.getDefaultMetricVisibility());
        }
        event.setAuxData(updatedQueryMetric);
        event.setRawRecordNumber(1000L);
        event.addAltId(updatedQueryMetric.getQueryId());
        
        event.setId(uidBuilder.newId(updatedQueryMetric.getQueryId().getBytes(Charset.forName("UTF-8")), (Date) null));
        
        final Multimap<String,NormalizedContentInterface> fields;
        
        if (deleteMode) {
            fields = ingestHelper.getEventFieldsToDelete(updatedQueryMetric, storedQueryMetric);
        } else {
            fields = ingestHelper.getEventFieldsToWrite(updatedQueryMetric, storedQueryMetric);
        }
        
        Key key = new Key();
        
        if (handler.getMetadata() != null) {
            handler.getMetadata().addEventWithoutLoadDates(ingestHelper, event, fields);
        }
        
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
            
            String value = currentKey.getRow().toString();
            if (value.length() > fieldSizeThreshold && (table.equals(indexTable) || table.equals(reverseIndexTable))) {
                keysToRemove.add(bulkIngestKey);
            }
        }
        
        // remove any keys from the index or reverseIndex where the value size exceeds the fieldSizeThreshold
        for (BulkIngestKey b : keysToRemove) {
            r.removeAll(b);
        }
        
        // replace the longest of the keys from
        // fields that get parsed as content
        for (Entry<String,BulkIngestKey> l : tfFields.entrySet()) {
            r.put(l.getValue(), new Value(new byte[0]));
        }
        
        for (Entry<BulkIngestKey,Collection<Value>> entry : r.asMap().entrySet()) {
            entry.getKey().getKey().setTimestamp(timestamp);
            entry.getKey().getKey().setDeleted(deleteMode);
        }
        
        return r;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public T combineMetrics(T updatedQueryMetric, T cachedQueryMetric, QueryMetricType metricType) throws Exception {
        return (T) queryMetricCombiner.combineMetrics(updatedQueryMetric, cachedQueryMetric, metricType);
    }
    
    public T getQueryMetric(final String queryId) throws Exception {
        return getQueryMetric(queryId, Collections.emptySet());
    }
    
    public List<T> getQueryMetrics(final String queryId) throws Exception {
        return getQueryMetrics(getJexlQuery(queryId), Collections.emptySet());
    }
    
    public T getQueryMetric(final String queryId, Collection<String> ignoredFields) throws Exception {
        List<T> queryMetrics = getQueryMetrics(getJexlQuery(queryId), ignoredFields);
        return queryMetrics.isEmpty() ? null : queryMetrics.get(0);
    }
    
    protected String getJexlQuery(final String queryId) {
        return "QUERY_ID == '" + queryId + "'";
    }
    
    public Query createQuery() {
        return new QueryImpl();
    }
    
    public List<T> getQueryMetrics(final String query, Collection<String> ignoredFields) throws Exception {
        Date end = new Date();
        Date begin = DateUtils.setYears(end, 2000);
        Query queryImpl = createQuery();
        queryImpl.setBeginDate(begin);
        queryImpl.setEndDate(end);
        queryImpl.setQueryLogicName(queryMetricHandlerProperties.getQueryMetricsLogic());
        queryImpl.setQuery(query);
        queryImpl.setQueryName(queryMetricHandlerProperties.getQueryMetricsLogic());
        queryImpl.setColumnVisibility(queryMetricHandlerProperties.getQueryVisibility());
        queryImpl.setQueryAuthorizations(this.clientAuthorizations);
        queryImpl.setExpirationDate(DateUtils.addDays(new Date(), 1));
        queryImpl.setPagesize(1000);
        queryImpl.setId(UUID.randomUUID());
        Map<String,String> parameters = new LinkedHashMap<>();
        parameters.put(QueryOptions.INCLUDE_GROUPING_CONTEXT, "true");
        parameters.put(QueryOptions.DATATYPE_FILTER, "querymetrics");
        if (ignoredFields != null && !ignoredFields.isEmpty()) {
            parameters.put(BLACKLISTED_FIELDS_DEPRECATED, StringUtils.join(ignoredFields, ","));
            parameters.put(QueryOptions.DISALLOWLISTED_FIELDS, StringUtils.join(ignoredFields, ","));
        }
        queryImpl.setParameters(parameters);
        return getQueryMetrics(queryImpl);
    }
    
    public List<T> getQueryMetrics(Query query) throws Exception {
        List<T> queryMetrics = new ArrayList<>();
        
        String queryId = query.getId().toString();
        try {
            BaseQueryResponse queryResponse = createAndNext(query);
            queryId = (queryResponse != null && queryResponse.getQueryId() != null) ? queryResponse.getQueryId() : queryId;
            
            boolean done = false;
            do {
                if (queryResponse != null) {
                    List<QueryExceptionType> exceptions = queryResponse.getExceptions();
                    if (queryResponse.getExceptions() != null && !queryResponse.getExceptions().isEmpty()) {
                        throw new RuntimeException(exceptions.get(0).getMessage());
                    }
                    
                    if (!(queryResponse instanceof EventQueryResponseBase)) {
                        throw new IllegalStateException("incompatible response");
                    }
                    
                    EventQueryResponseBase eventQueryResponse = (EventQueryResponseBase) queryResponse;
                    List<EventBase> eventList = eventQueryResponse.getEvents();
                    
                    if (eventList != null && !eventList.isEmpty()) {
                        for (EventBase<?,?> event : eventList) {
                            T metric = toMetric(event);
                            queryMetrics.add(metric);
                        }
                        
                        // request the next page
                        queryResponse = next(queryId);
                    } else {
                        done = true;
                    }
                } else {
                    done = true;
                }
            } while (!done);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        } finally {
            close(queryId);
        }
        
        return queryMetrics;
    }
    
    protected abstract BaseQueryResponse createAndNext(Query query) throws Exception;
    
    protected abstract BaseQueryResponse next(String queryId) throws Exception;
    
    protected abstract void close(String queryId);
    
    public T toMetric(EventBase event) {
        SimpleDateFormat sdf_date_time1 = new SimpleDateFormat("yyyyMMdd HHmmss");
        SimpleDateFormat sdf_date_time2 = new SimpleDateFormat("yyyyMMdd HHmmss");
        SimpleDateFormat sdf_date_time3 = new SimpleDateFormat("yyyyMMdd");
        
        List<String> excludedFields = Arrays.asList("ELAPSED_TIME", "RECORD_ID", "NUM_PAGES", "NUM_RESULTS");
        
        try {
            T m = (T) metricFactory.createMetric(false);
            List<FieldBase> field = event.getFields();
            m.setMarkings(event.getMarkings());
            TreeMap<Long,PageMetric> pageMetrics = Maps.newTreeMap();
            
            boolean createDateSet = false;
            for (FieldBase f : field) {
                String fieldName = f.getName();
                String fieldValue = f.getValueString();
                if (!excludedFields.contains(fieldName)) {
                    
                    if (fieldName.equals("AUTHORIZATIONS")) {
                        m.setQueryAuthorizations(fieldValue);
                    } else if (fieldName.equals("BEGIN_DATE")) {
                        try {
                            Date d = sdf_date_time1.parse(fieldValue);
                            m.setBeginDate(d);
                        } catch (Exception e) {
                            log.error("{}:{}:{}", fieldName, fieldValue, e.getMessage());
                        }
                    } else if (fieldName.equals("CREATE_CALL_TIME")) {
                        m.setCreateCallTime(Long.parseLong(fieldValue));
                    } else if (fieldName.equals("CREATE_DATE")) {
                        try {
                            Date d = sdf_date_time2.parse(fieldValue);
                            m.setCreateDate(d);
                            createDateSet = true;
                        } catch (Exception e) {
                            log.error("{}:{}:{}", fieldName, fieldValue, e.getMessage());
                        }
                    } else if (fieldName.equals("DOC_RANGES")) {
                        try {
                            long l = Long.parseLong(fieldValue);
                            if (l > m.getDocRanges()) {
                                m.setDocRanges(l);
                            }
                        } catch (Exception e) {
                            log.error("{}:{}:{}", fieldName, fieldValue, e.getMessage());
                        }
                    } else if (fieldName.equals("DOC_SIZE")) {
                        try {
                            long l = Long.parseLong(fieldValue);
                            if (l > m.getDocSize()) {
                                m.setDocSize(l);
                            }
                        } catch (Exception e) {
                            log.error("{}:{}:{}", fieldName, fieldValue, e.getMessage());
                        }
                    } else if (fieldName.equals("END_DATE")) {
                        try {
                            Date d = sdf_date_time1.parse(fieldValue);
                            m.setEndDate(d);
                        } catch (Exception e) {
                            log.error("{}:{}:{}", fieldName, fieldValue, e.getMessage());
                        }
                    } else if (fieldName.equals("ERROR_CODE")) {
                        m.setErrorCode(fieldValue);
                    } else if (fieldName.equals("ERROR_MESSAGE")) {
                        m.setErrorMessage(fieldValue);
                    } else if (fieldName.equals("FI_RANGES")) {
                        try {
                            long l = Long.parseLong(fieldValue);
                            if (l > m.getFiRanges()) {
                                m.setFiRanges(l);
                            }
                        } catch (Exception e) {
                            log.error("{}:{}:{}", fieldName, fieldValue, e.getMessage());
                        }
                    } else if (fieldName.equals("HOST")) {
                        m.setHost(fieldValue);
                    } else if (fieldName.equals("LAST_UPDATED")) {
                        try {
                            Date d = sdf_date_time2.parse(fieldValue);
                            // protect against multiple values by coosing the latest
                            if (m.getLastUpdated() == null || d.after(m.getLastUpdated())) {
                                m.setLastUpdated(d);
                            }
                        } catch (Exception e) {
                            log.error("{}:{}:{}", fieldName, fieldValue, e.getMessage());
                        }
                    } else if (fieldName.equals("LIFECYCLE")) {
                        Lifecycle l = Lifecycle.valueOf(fieldValue);
                        // protect against multiple values by choosing the last by ordinal
                        if (m.getLifecycle() == null || l.ordinal() > m.getLifecycle().ordinal()) {
                            m.setLifecycle(Lifecycle.valueOf(fieldValue));
                        }
                    } else if (fieldName.equals("LOGIN_TIME")) {
                        m.setLoginTime(Long.parseLong(fieldValue));
                    } else if (fieldName.equals("NEGATIVE_SELECTORS")) {
                        List<String> negativeSelectors = m.getNegativeSelectors();
                        if (negativeSelectors == null) {
                            negativeSelectors = new ArrayList<>();
                        }
                        negativeSelectors.add(fieldValue);
                        m.setNegativeSelectors(negativeSelectors);
                    } else if (fieldName.equals("NEXT_COUNT")) {
                        try {
                            long l = Long.parseLong(fieldValue);
                            if (l > m.getNextCount()) {
                                m.setNextCount(l);
                            }
                        } catch (Exception e) {
                            log.error("{}:{}:{}", fieldName, fieldValue, e.getMessage());
                        }
                    } else if (fieldName.equals("NUM_UPDATES")) {
                        try {
                            long l = Long.parseLong(fieldValue);
                            if (l > m.getNumUpdates()) {
                                m.setNumUpdates(l);
                            }
                        } catch (Exception e) {
                            log.error("{}:{}:{}", fieldName, fieldValue, e.getMessage());
                        }
                    } else if (fieldName.startsWith("PAGE_METRICS")) {
                        int index = fieldName.indexOf(".");
                        if (-1 == index) {
                            log.error("Could not parse field name to extract repetition count: {}", fieldName);
                        } else {
                            long pageNum = Long.parseLong(fieldName.substring(index + 1));
                            PageMetric pageMetric = PageMetric.parse(fieldValue);
                            if (pageMetric != null) {
                                pageMetric.setPageNumber(pageNum);
                                pageMetrics.put(pageNum, pageMetric);
                            }
                        }
                    } else if (fieldName.equals("PARAMETERS")) {
                        if (fieldValue != null) {
                            try {
                                m.setParameters(QueryUtil.parseParameters(fieldValue));
                            } catch (Exception e) {
                                log.error(e.getMessage());
                            }
                        }
                    } else if (fieldName.equals("PLAN")) {
                        m.setPlan(fieldValue);
                    } else if (fieldName.equals("POSITIVE_SELECTORS")) {
                        List<String> positiveSelectors = m.getPositiveSelectors();
                        if (positiveSelectors == null) {
                            positiveSelectors = new ArrayList<>();
                        }
                        positiveSelectors.add(fieldValue);
                        m.setPositiveSelectors(positiveSelectors);
                    } else if (fieldName.equals("PREDICTION")) {
                        if (fieldValue != null) {
                            try {
                                int x = fieldValue.indexOf(":");
                                if (x > -1) {
                                    String predictionName = fieldValue.substring(0, x);
                                    double predictionValue = Double.parseDouble(fieldValue.substring(x + 1));
                                    m.addPrediction(new Prediction(predictionName, predictionValue));
                                }
                            } catch (Exception e) {
                                log.error(e.getMessage());
                            }
                        }
                    } else if (fieldName.equals("PROXY_SERVERS")) {
                        m.setProxyServers(Arrays.asList(StringUtils.split(fieldValue, ",")));
                    } else if (fieldName.equals("QUERY")) {
                        m.setQuery(fieldValue);
                    } else if (fieldName.equals("QUERY_ID")) {
                        m.setQueryId(fieldValue);
                    } else if (fieldName.equals("QUERY_LOGIC")) {
                        m.setQueryLogic(fieldValue);
                    } else if (fieldName.equals("QUERY_NAME")) {
                        m.setQueryName(fieldValue);
                    } else if (fieldName.equals("QUERY_TYPE")) {
                        m.setQueryType(fieldValue);
                    } else if (fieldName.equals("SEEK_COUNT")) {
                        try {
                            long l = Long.parseLong(fieldValue);
                            if (l > m.getSeekCount()) {
                                m.setSeekCount(l);
                            }
                        } catch (Exception e) {
                            log.error("{}:{}:{}", fieldName, fieldValue, e.getMessage());
                        }
                    } else if (fieldName.equals("SETUP_TIME")) {
                        m.setSetupTime(Long.parseLong(fieldValue));
                    } else if (fieldName.equals("SOURCE_COUNT")) {
                        try {
                            long l = Long.parseLong(fieldValue);
                            if (l > m.getSourceCount()) {
                                m.setSourceCount(l);
                            }
                        } catch (Exception e) {
                            log.error("{}:{}:{}", fieldName, fieldValue, e.getMessage());
                        }
                    } else if (fieldName.equals("USER")) {
                        m.setUser(fieldValue);
                    } else if (fieldName.equals("USER_DN")) {
                        m.setUserDN(fieldValue);
                    } else if (fieldName.equals("VERSION")) {
                        m.addVersion(BaseQueryMetric.DATAWAVE, fieldValue);
                    } else if (fieldName.startsWith("VERSION.")) {
                        m.addVersion(fieldName.substring(8), fieldValue);
                    } else if (fieldName.equals("YIELD_COUNT")) {
                        try {
                            long l = Long.parseLong(fieldValue);
                            if (l > m.getYieldCount()) {
                                m.setYieldCount(l);
                            }
                        } catch (Exception e) {
                            log.error("{}:{}:{}", fieldName, fieldValue, e.getMessage());
                        }
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
        } catch (RuntimeException e) {
            return null;
        }
    }
    
    protected void createAndConfigureTablesIfNecessary(String[] tableNames, AccumuloClient accumuloClient, Configuration conf)
                    throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        
        for (String table : tableNames) {
            // If the tables don't exist, then create them.
            try {
                String[] tableNameSplit = StringUtils.split(table, '.');
                if (tableNameSplit.length > 1) {
                    String namespace = tableNameSplit[0];
                    if (!accumuloClient.namespaceOperations().exists(namespace)) {
                        try {
                            accumuloClient.namespaceOperations().create(namespace);
                        } catch (NamespaceExistsException e) {
                            log.error(e.getMessage());
                        }
                    }
                }
                if (!accumuloClient.tableOperations().exists(table)) {
                    accumuloClient.tableOperations().create(table);
                }
            } catch (TableExistsException te) {
                // in this case, somebody else must have created the table after our existence check
                log.debug("Tried to create {} but it already exists", table);
            }
            try {
                TableConfigHelper tableHelper = getTableConfig(conf, table);
                if (tableHelper != null) {
                    tableHelper.configure(accumuloClient.tableOperations());
                } else {
                    log.info("No configuration supplied for table: {}", table);
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }
    
    @SuppressWarnings("unchecked")
    protected TableConfigHelper getTableConfig(Configuration conf, String tableName) {
        String prop = tableName + TableConfigHelper.TABLE_CONFIG_CLASS_SUFFIX;
        String className = conf.get(prop, null);
        TableConfigHelper tableHelper = null;
        
        if (className != null) {
            try {
                Class<? extends TableConfigHelper> tableHelperClass = (Class<? extends TableConfigHelper>) Class.forName(className.trim());
                tableHelper = tableHelperClass.getDeclaredConstructor().newInstance();
                
                if (tableHelper != null) {
                    tableHelper.setup(tableName, conf, setupLogger);
                }
            } catch (Exception e) {
                throw new IllegalArgumentException(e);
            }
        }
        return tableHelper;
    }
    
    @Override
    public void reload() {
        this.accumuloRecordWriterLock.writeLock().lock();
        try {
            if (this.recordWriter == null || !this.recordWriter.isHealthy()) {
                if (this.recordWriter != null) {
                    // If recordWriter != null then this method is being called because of an error writing to Accumulo.
                    // We have to try to close the recordWriter and therefore the mtbw because Accumulo now reference
                    // counts certain objects (like mtbw) to ensure that they are closed before being dereferenced.
                    try {
                        this.recordWriter.close(null);
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                }
                this.recordWriter = new AccumuloRecordWriter(accumuloClientPool, conf);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            this.accumuloRecordWriterLock.writeLock().unlock();
        }
    }
    
    @Override
    public ContentQueryMetricsIngestHelper getQueryMetricsIngestHelper(boolean deleteMode, Collection<String> ignoredFields) {
        return new ContentQueryMetricsIngestHelper(deleteMode, ignoredFields);
    }
    
    @Override
    public QueryMetricsSummaryResponse getQueryMetricsSummary(Date begin, Date end, DatawaveUserDetails currentUser, boolean onlyCurrentUser) {
        QueryMetricsSummaryResponse response = createSummaryResponse();
        try {
            // this method is open to any user
            DatawaveUser datawaveUser = currentUser.getPrimaryUser();
            String datawaveUserShortName = dnUtils.getShortName(datawaveUser.getName());
            Collection<String> userAuths = new ArrayList<>(datawaveUser.getAuths());
            if (clientAuthorizations != null) {
                Collection<String> connectorAuths = new ArrayList<>();
                Arrays.stream(StringUtils.split(clientAuthorizations, ',')).forEach(a -> {
                    connectorAuths.add(a);
                });
                userAuths.retainAll(connectorAuths);
            }
            Collection<? extends Collection<String>> authorizations = Collections.singletonList(userAuths);
            Query query = createQuery();
            query.setBeginDate(begin);
            query.setEndDate(end);
            query.setQueryLogicName(queryMetricHandlerProperties.getQueryMetricsLogic());
            if (onlyCurrentUser) {
                query.setQuery("USER == '" + datawaveUserShortName + "'");
            } else {
                query.setQuery("((_Bounded_ = true) && (USER > 'A' && USER < 'ZZZZZZZ'))");
            }
            query.setQueryName(queryMetricHandlerProperties.getQueryMetricsLogic());
            query.setColumnVisibility(queryMetricHandlerProperties.getQueryVisibility());
            query.setQueryAuthorizations(WSAuthorizationsUtil.buildAuthorizationString(authorizations));
            query.setExpirationDate(DateUtils.addDays(new Date(), 1));
            query.setPagesize(1000);
            query.setUserDN(datawaveUserShortName);
            query.setId(UUID.randomUUID());
            Map<String,String> parameters = new LinkedHashMap<>();
            parameters.put(QueryOptions.INCLUDE_GROUPING_CONTEXT, "true");
            parameters.put(QueryParameters.DATATYPE_FILTER_SET, "querymetrics");
            query.setParameters(parameters);
            
            List<T> queryMetrics = getQueryMetrics(query);
            response = processQueryMetricsSummary(queryMetrics, end);
            
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            response.addException(e);
        }
        return response;
    }
}
