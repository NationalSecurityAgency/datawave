package datawave.microservice.querymetric;

import static datawave.microservice.querymetric.config.HazelcastMetricCacheConfiguration.INCOMING_METRICS;
import static datawave.security.authorization.DatawaveUser.UserType.USER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Named;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.messaging.Message;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.map.IMap;
import com.hazelcast.spring.cache.HazelcastCacheManager;

import datawave.core.common.connection.AccumuloClientPool;
import datawave.ingest.protobuf.Uid;
import datawave.marking.MarkingFunctions;
import datawave.microservice.authorization.preauth.ProxiedEntityX509Filter;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.querymetric.config.QueryMetricClientProperties;
import datawave.microservice.querymetric.config.QueryMetricHandlerProperties;
import datawave.microservice.querymetric.function.QueryMetricSupplier;
import datawave.microservice.querymetric.handler.AccumuloClientTracking;
import datawave.microservice.querymetric.handler.QueryMetricCombiner;
import datawave.microservice.querymetric.handler.ShardTableQueryMetricHandler;
import datawave.microservice.security.util.DnUtils;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.JWTTokenHandler;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.webservice.query.result.event.DefaultEvent;
import datawave.webservice.query.result.event.DefaultField;
import datawave.webservice.query.result.event.EventBase;

public class QueryMetricTestBase {
    
    protected Logger log = LoggerFactory.getLogger(getClass());
    
    protected static final SubjectIssuerDNPair ALLOWED_CALLER = SubjectIssuerDNPair.of("cn=test a. user, ou=example developers, o=example corp, c=us",
                    "cn=example corp ca, o=example corp, c=us");
    
    protected static final String getMetricsUrl = "/querymetric/v1/id/%s";
    
    @Autowired
    protected QueryMetricOperations queryMetricOperations;
    
    @Autowired
    protected RestTemplateBuilder restTemplateBuilder;
    
    @Autowired
    protected JWTTokenHandler jwtTokenHandler;
    
    @Autowired
    protected ObjectMapper objectMapper;
    
    @Autowired
    protected ShardTableQueryMetricHandler shardTableQueryMetricHandler;
    
    @Autowired
    protected QueryMetricCombiner queryMetricCombiner;
    
    @Autowired
    @Named("queryMetricCacheManager")
    protected CacheManager cacheManager;
    
    @Autowired
    protected @Qualifier("warehouse") AccumuloClientPool accumuloClientPool;
    
    @Autowired
    protected QueryMetricHandlerProperties queryMetricHandlerProperties;
    
    @Autowired
    protected QueryMetricFactory queryMetricFactory;
    
    @Autowired
    protected QueryMetricClient client;
    
    @Autowired
    private QueryMetricClientProperties queryMetricClientProperties;
    
    @Autowired
    private DnUtils dnUtils;
    
    @Autowired
    @Qualifier("lastWrittenQueryMetrics")
    protected Cache lastWrittenQueryMetricCache;
    
    protected Cache incomingQueryMetricsCache;
    
    @LocalServerPort
    protected int webServicePort;
    
    protected RestTemplate restTemplate;
    protected DatawaveUserDetails adminUser;
    protected DatawaveUserDetails nonAdminUser;
    protected static boolean isHazelCast;
    protected static CacheManager staticCacheManager;
    protected static Map<String,String> metricMarkings;
    protected List<String> tables;
    protected Collection<String> auths;
    protected AccumuloClient accumuloClient;
    
    static {
        metricMarkings = new HashMap<>();
        metricMarkings.put(MarkingFunctions.Default.COLUMN_VISIBILITY, "A&C");
    }
    
    @AfterAll
    public static void afterClass() {
        ((HazelcastCacheManager) staticCacheManager).getHazelcastInstance().shutdown();
    }
    
    @BeforeEach
    public void setup() {
        this.queryMetricClientProperties.setPort(webServicePort);
        this.restTemplate = restTemplateBuilder.build(RestTemplate.class);
        this.auths = Arrays.asList("PUBLIC", "A", "B", "C");
        
        Collection<String> roles = Arrays.asList("Administrator");
        DatawaveUser adminDWUser = new DatawaveUser(ALLOWED_CALLER, USER, null, auths, roles, null, System.currentTimeMillis());
        DatawaveUser nonAdminDWUser = new DatawaveUser(ALLOWED_CALLER, USER, null, auths, null, null, System.currentTimeMillis());
        this.adminUser = new DatawaveUserDetails(Collections.singleton(adminDWUser), adminDWUser.getCreationTime());
        this.nonAdminUser = new DatawaveUserDetails(Collections.singleton(nonAdminDWUser), nonAdminDWUser.getCreationTime());
        QueryMetricTestBase.isHazelCast = cacheManager instanceof HazelcastCacheManager;
        QueryMetricTestBase.staticCacheManager = cacheManager;
        this.incomingQueryMetricsCache = cacheManager.getCache(INCOMING_METRICS);
        this.shardTableQueryMetricHandler.verifyTables();
        BaseQueryMetric m = createMetric();
        // this is to ensure that the QueryMetrics_m table
        // is populated so that queries work properly
        try {
            this.shardTableQueryMetricHandler.writeMetric(m, Collections.emptyList(), m.getCreateDate().getTime(), false);
            this.shardTableQueryMetricHandler.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.tables = new ArrayList<>();
        this.tables.add(queryMetricHandlerProperties.getIndexTableName());
        this.tables.add(queryMetricHandlerProperties.getReverseIndexTableName());
        this.tables.add(queryMetricHandlerProperties.getShardTableName());
        try {
            Map<String,String> trackingMap = AccumuloClientTracking.getTrackingMap(Thread.currentThread().getStackTrace());
            this.accumuloClient = this.accumuloClientPool.borrowObject(trackingMap);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        deleteAccumuloEntries(this.accumuloClient, this.tables, this.auths);
        assertTrue(getMetadataEntries().size() > 0, "metadata table empty");
        SimpleModule baseQueryMetricDeserializer = new SimpleModule(BaseQueryMetricListResponse.class.getName());
        baseQueryMetricDeserializer.addAbstractTypeMapping(BaseQueryMetricListResponse.class, QueryMetricListResponse.class);
        this.objectMapper.registerModule(baseQueryMetricDeserializer);
    }
    
    @AfterEach
    public void cleanup() {
        deleteAccumuloEntries(this.accumuloClient, this.tables, this.auths);
        this.incomingQueryMetricsCache.clear();
        this.lastWrittenQueryMetricCache.clear();
        this.accumuloClientPool.returnObject(this.accumuloClient);
    }
    
    protected EventBase toEvent(BaseQueryMetric metric) {
        SimpleDateFormat sdf_date_time1 = new SimpleDateFormat("yyyyMMdd HHmmss");
        SimpleDateFormat sdf_date_time2 = new SimpleDateFormat("yyyyMMdd HHmmss");
        
        long createTime = metric.getCreateDate().getTime();
        
        DefaultEvent event = new DefaultEvent();
        List<DefaultField> fields = new ArrayList<>();
        
        event.setMarkings(metric.getMarkings());
        
        addStringField(fields, "QUERY_ID", metric.getColumnVisibility(), createTime, metric.getQueryId());
        addDateField(fields, "BEGIN_DATE", metric.getColumnVisibility(), createTime, metric.getBeginDate(), sdf_date_time1);
        addDateField(fields, "END_DATE", metric.getColumnVisibility(), createTime, metric.getEndDate(), sdf_date_time1);
        addDateField(fields, "LAST_UPDATED", metric.getColumnVisibility(), createTime, metric.getLastUpdated(), sdf_date_time2);
        addLongField(fields, "NUM_UPDATES", metric.getColumnVisibility(), createTime, metric.getNumUpdates());
        addStringField(fields, "QUERY", metric.getColumnVisibility(), createTime, metric.getQuery());
        addStringField(fields, "QUERY_LOGIC", metric.getColumnVisibility(), createTime, metric.getQueryLogic());
        addStringField(fields, "HOST", metric.getColumnVisibility(), createTime, metric.getHost());
        addStringField(fields, "QUERY_TYPE", metric.getColumnVisibility(), createTime, metric.getQueryType());
        addLifecycleField(fields, "LIFECYCLE", metric.getColumnVisibility(), createTime, metric.getLifecycle());
        addLongField(fields, "LOGIN_TIME", metric.getColumnVisibility(), createTime, metric.getLoginTime());
        addDateField(fields, "CREATE_DATE", metric.getColumnVisibility(), createTime, metric.getCreateDate(), sdf_date_time2);
        addLongField(fields, "CREATE_CALL_TIME", metric.getColumnVisibility(), createTime, metric.getCreateCallTime());
        addStringField(fields, "AUTHORIZATIONS", metric.getColumnVisibility(), createTime, metric.getQueryAuthorizations());
        addStringField(fields, "QUERY_NAME", metric.getColumnVisibility(), createTime, metric.getQueryName());
        addLongField(fields, "DOC_RANGES", metric.getColumnVisibility(), createTime, metric.getDocRanges());
        addLongField(fields, "FI_RANGES", metric.getColumnVisibility(), createTime, metric.getFiRanges());
        addStringField(fields, "ERROR_CODE", metric.getColumnVisibility(), createTime, metric.getErrorCode());
        addStringField(fields, "ERROR_MESSAGE", metric.getColumnVisibility(), createTime, metric.getErrorMessage());
        addLongField(fields, "SEEK_COUNT", metric.getColumnVisibility(), createTime, metric.getSeekCount());
        addLongField(fields, "NEXT_COUNT", metric.getColumnVisibility(), createTime, metric.getNextCount());
        addStringField(fields, "USER", metric.getColumnVisibility(), createTime, metric.getUser());
        addStringField(fields, "USER_DN", metric.getColumnVisibility(), createTime, metric.getUserDN());
        addPredictionField(fields, metric.getColumnVisibility(), createTime, metric.getPredictions());
        addStringField(fields, "PLAN", metric.getColumnVisibility(), createTime, metric.getPlan());
        addPageMetricsField(fields, metric.getColumnVisibility(), createTime, metric.getPageTimes());
        
        event.setFields(fields);
        
        return event;
    }
    
    protected void addPageMetricsField(List<DefaultField> fields, String columnVisibility, long timestamp, List<BaseQueryMetric.PageMetric> pageMetrics) {
        if (pageMetrics != null) {
            int page = 1;
            for (BaseQueryMetric.PageMetric pageMetric : pageMetrics) {
                addStringField(fields, "PAGE_METRICS." + page++, columnVisibility, timestamp, pageMetric.toEventString());
            }
        }
    }
    
    protected void addStringField(List<DefaultField> fields, String field, String columnVisibility, long timestamp, String value) {
        if (value != null) {
            fields.add(new DefaultField(field, columnVisibility, timestamp, value));
        }
    }
    
    protected void addLifecycleField(List<DefaultField> fields, String field, String columnVisibility, long timestamp, BaseQueryMetric.Lifecycle value) {
        if (value != null) {
            fields.add(new DefaultField(field, columnVisibility, timestamp, value.name()));
        }
    }
    
    protected void addLongField(List<DefaultField> fields, String field, String columnVisibility, long timestamp, Long value) {
        if (value != null) {
            fields.add(new DefaultField(field, columnVisibility, timestamp, Long.toString(value)));
        }
    }
    
    protected void addDateField(List<DefaultField> fields, String field, String columnVisibility, long timestamp, Date value, SimpleDateFormat sdf) {
        if (value != null) {
            fields.add(new DefaultField(field, columnVisibility, timestamp, sdf.format(value)));
        }
    }
    
    protected void addPredictionField(List<DefaultField> fields, String columnVisibility, long timestamp, Set<BaseQueryMetric.Prediction> value) {
        if (value != null) {
            for (BaseQueryMetric.Prediction prediction : value) {
                if (prediction != null) {
                    addStringField(fields, "PREDICTION", columnVisibility, timestamp,
                                    String.join(":", prediction.getName(), Double.toString(prediction.getPrediction())));
                }
            }
        }
    }
    
    protected BaseQueryMetric createMetric() {
        return createMetric(createQueryId());
    }
    
    protected static BaseQueryMetric createMetric(QueryMetricFactory queryMetricFactory) {
        return createMetric(createQueryId(), queryMetricFactory);
    }
    
    protected BaseQueryMetric createMetric(String queryId) {
        return createMetric(queryId, this.queryMetricFactory);
    }
    
    protected static BaseQueryMetric createMetric(String queryId, QueryMetricFactory queryMetricFactory) {
        BaseQueryMetric m = queryMetricFactory.createMetric();
        populateMetric(m, queryId);
        return m;
    }
    
    protected static void populateMetric(BaseQueryMetric m, String queryId) {
        long now = System.currentTimeMillis();
        Date nowDate = new Date(now);
        m.setQueryId(queryId);
        m.setMarkings(metricMarkings);
        m.setEndDate(nowDate);
        m.setBeginDate(DateUtils.addDays(nowDate, -1));
        m.setLastUpdated(nowDate);
        m.setQuery("USER:testuser");
        m.setQueryLogic("QueryMetricsQuery");
        m.setHost("localhost");
        m.setQueryType("RunningQuery");
        m.setLifecycle(BaseQueryMetric.Lifecycle.INITIALIZED);
        m.setSetupTime(4000);
        m.setCreateCallTime(4000);
        m.setQueryAuthorizations("A,B,C");
        m.setQueryName("TestQuery");
        m.setDocRanges(300);
        m.setNextCount(300);
        m.setSeekCount(300);
        m.setUser(DnUtils.getShortName(ALLOWED_CALLER.subjectDN()));
        m.setUserDN(ALLOWED_CALLER.subjectDN());
        m.addPrediction(new BaseQueryMetric.Prediction("PredictionTest", 200.0));
    }
    
    public static String createQueryId() {
        StringBuilder sb = new StringBuilder();
        sb.append(RandomStringUtils.randomNumeric(4));
        sb.append("-");
        sb.append(RandomStringUtils.randomNumeric(4));
        sb.append("-");
        sb.append(RandomStringUtils.randomNumeric(4));
        sb.append("-");
        sb.append(RandomStringUtils.randomNumeric(4));
        return sb.toString();
    }
    
    protected HttpEntity createRequestEntity(DatawaveUserDetails trustedUser, DatawaveUserDetails jwtUser, Object body) throws JsonProcessingException {
        
        HttpHeaders headers = new HttpHeaders();
        if (this.jwtTokenHandler != null && jwtUser != null) {
            String token = this.jwtTokenHandler.createTokenFromUsers(jwtUser.getUsername(), jwtUser.getProxiedUsers());
            headers.add("Authorization", "Bearer " + token);
        }
        if (trustedUser != null) {
            headers.add(ProxiedEntityX509Filter.SUBJECT_DN_HEADER, trustedUser.getPrimaryUser().getDn().subjectDN());
            headers.add(ProxiedEntityX509Filter.ISSUER_DN_HEADER, trustedUser.getPrimaryUser().getDn().issuerDN());
        }
        headers.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);
        if (body == null) {
            return new HttpEntity<>(null, headers);
        } else {
            return new HttpEntity<>(this.objectMapper.writeValueAsString(body), headers);
        }
    }
    
    public static void metricAssertEquals(BaseQueryMetric m1, BaseQueryMetric m2) {
        metricAssertEquals("", m1, m2);
    }
    
    /*
     * This method compares the fields of BaseQueryMetric one by one so that the discrepancy is obvious It also rounds all Date objects to
     */
    public static void metricAssertEquals(String message, BaseQueryMetric m1, BaseQueryMetric m2) {
        if (null == m2) {
            fail(message + ": actual metric is null");
        } else if (m1 == m2) {
            return;
        } else {
            if (message == null || message.isEmpty()) {
                message = "";
            } else {
                message = message + ": ";
            }
            assertTrue(assertObjectsEqual(m1.getQueryId(), m2.getQueryId()), message + "queryId");
            assertTrue(assertObjectsEqual(m1.getQueryType(), m2.getQueryType()), message + "queryType");
            assertTrue(assertObjectsEqual(m1.getQueryAuthorizations(), m2.getQueryAuthorizations()), message + "queryAuthorizations");
            assertTrue(assertObjectsEqual(m1.getColumnVisibility(), m2.getColumnVisibility()), message + "columnVisibility");
            assertEquals(m1.getMarkings(), m2.getMarkings(), message + "markings");
            assertTrue(assertObjectsEqual(m1.getBeginDate(), m2.getBeginDate()), message + "beginDate");
            assertTrue(assertObjectsEqual(m1.getEndDate(), m2.getEndDate()), message + "endDate");
            assertEquals(m1.getCreateDate(), m2.getCreateDate(), message + "createDate");
            assertEquals(m1.getSetupTime(), m2.getSetupTime(), message + "setupTime");
            assertEquals(m1.getCreateCallTime(), m2.getCreateCallTime(), message + "createCallTime");
            assertTrue(assertObjectsEqual(m1.getUser(), m2.getUser()), message + "user");
            assertTrue(assertObjectsEqual(m1.getUserDN(), m2.getUserDN()), message + "userDN");
            assertTrue(assertObjectsEqual(m1.getQuery(), m2.getQuery()), message + "query");
            assertTrue(assertObjectsEqual(m1.getQueryLogic(), m2.getQueryLogic()), message + "queryLogic");
            assertTrue(assertObjectsEqual(m1.getQueryName(), m2.getQueryName()), message + "queryName");
            assertTrue(assertObjectsEqual(m1.getParameters(), m2.getParameters()), message + "parameters");
            assertTrue(assertObjectsEqual(m1.getHost(), m2.getHost()), message + "host");
            assertTrue(assertObjectsEqual(m1.getPageTimes(), m2.getPageTimes()), message + "pageTimes");
            assertTrue(assertObjectsEqual(m1.getProxyServers(), m2.getProxyServers()), message + "proxyServers");
            assertTrue(assertObjectsEqual(m1.getLifecycle(), m2.getLifecycle()), message + "lifecycle");
            assertTrue(assertObjectsEqual(m1.getErrorMessage(), m2.getErrorMessage()), message + "errorMessage");
            assertTrue(assertObjectsEqual(m1.getErrorCode(), m2.getErrorCode()), message + "errorCode");
            assertEquals(m1.getSourceCount(), m2.getSourceCount(), message + "sourceCount");
            assertEquals(m1.getNextCount(), m2.getNextCount(), message + "nextCount");
            assertEquals(m1.getSeekCount(), m2.getSeekCount(), message + "seekCount");
            assertEquals(m1.getYieldCount(), m2.getYieldCount(), message + "yieldCount");
            assertEquals(m1.getDocRanges(), m2.getDocRanges(), message + "docRanges");
            assertEquals(m1.getFiRanges(), m2.getFiRanges(), message + "fiRanges");
            assertTrue(assertObjectsEqual(m1.getPlan(), m2.getPlan()), message + "plan");
            assertEquals(m1.getLoginTime(), m2.getLoginTime(), message + "loginTime");
            assertTrue(assertObjectsEqual(m1.getPredictions(), m2.getPredictions()), message + "predictions");
            assertEquals(m1.getVersionMap(), m2.getVersionMap(), message + "versionMap");
        }
    }
    
    public static boolean assertObjectsEqual(Object o1, Object o2) {
        if (o1 == null && o2 == null) {
            return true;
        } else if (o1 == null && o2 != null) {
            return false;
        } else if (o1 != null && o2 == null) {
            return false;
        } else if (o1.getClass() != o2.getClass()) {
            return false;
        } else if (o1 instanceof Date) {
            return datesEqual((Date) o1, (Date) o2);
        } else {
            return o1.equals(o2);
        }
    }
    
    public static boolean datesEqual(Date d1, Date d2) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HHmmss");
        return sdf.format(d1).equals(sdf.format(d2));
    }
    
    public static String formatDate(Date d) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HHmmss");
        return sdf.format(d);
    }
    
    protected Collection<String> getAllAccumuloEntries() {
        List<String> entries = new ArrayList<>();
        List<String> tables = new ArrayList<>();
        tables.add(this.queryMetricHandlerProperties.getShardTableName());
        tables.add(this.queryMetricHandlerProperties.getIndexTableName());
        tables.add(this.queryMetricHandlerProperties.getReverseIndexTableName());
        tables.forEach(t -> {
            entries.addAll(getAccumuloEntryStrings(t));
        });
        return entries;
    }
    
    protected Collection<String> getMetadataEntries() {
        return getAccumuloEntryStrings(this.queryMetricHandlerProperties.getMetadataTableName());
    }
    
    protected Collection<String> getAccumuloEntryStrings(String table) {
        List<String> entryStrings = new ArrayList<>();
        try {
            Collection<Map.Entry<Key,Value>> entries = getAccumuloEntries(this.accumuloClient, table, this.auths);
            for (Map.Entry<Key,Value> e : entries) {
                entryStrings.add(table + " -> " + e.getKey());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return entryStrings;
    }
    
    protected void printAllAccumuloEntries() {
        getAllAccumuloEntries().forEach(s -> System.out.println(s));
    }
    
    public static Collection<Map.Entry<Key,Value>> getAccumuloEntries(AccumuloClient accumuloClient, String table, Collection<String> authorizations)
                    throws Exception {
        Collection<Map.Entry<Key,Value>> entries = new ArrayList<>();
        String[] authArray = new String[authorizations.size()];
        authorizations.toArray(authArray);
        Authorizations auths = new Authorizations(authArray);
        try (BatchScanner bs = accumuloClient.createBatchScanner(table, auths, 1)) {
            bs.setRanges(Collections.singletonList(new Range()));
            final Iterator<Map.Entry<Key,Value>> itr = bs.iterator();
            while (itr.hasNext()) {
                entries.add(itr.next());
            }
        }
        return entries;
    }
    
    public static void deleteAccumuloEntries(AccumuloClient accumuloClient, List<String> tables, Collection<String> authorizations) {
        try {
            String[] authArray = new String[authorizations.size()];
            authorizations.toArray(authArray);
            tables.forEach(t -> {
                Authorizations auths = new Authorizations(authArray);
                try (BatchDeleter bd = accumuloClient.createBatchDeleter(t, auths, 1, new BatchWriterConfig())) {
                    bd.setRanges(Collections.singletonList(new Range()));
                    bd.delete();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    protected void ensureDataStored(Cache incomingCache, String queryId) {
        long now = System.currentTimeMillis();
        int writeDelaySeconds = 1000;
        boolean found = false;
        IMap<Object,Object> hzCache = ((IMap<Object,Object>) incomingCache.getNativeCache());
        while (!found && System.currentTimeMillis() < (now + (1000 * (writeDelaySeconds + 1)))) {
            found = hzCache.containsKey(queryId);
            if (!found) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {}
            }
        }
    }
    
    protected void ensureDataWritten(Cache incomingCache, Cache lastWrittenCache, String queryId) {
        long now = System.currentTimeMillis();
        Config config = ((HazelcastCacheManager) this.cacheManager).getHazelcastInstance().getConfig();
        MapStoreConfig mapStoreConfig = config.getMapConfig(incomingCache.getName()).getMapStoreConfig();
        int writeDelaySeconds = Math.min(mapStoreConfig.getWriteDelaySeconds(), 1000);
        boolean found = false;
        while (!found && System.currentTimeMillis() < (now + (1000 * (writeDelaySeconds + 5.0)))) {
            found = lastWrittenCache.get(queryId, QueryMetricUpdateHolder.class) != null;
            if (!found) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {}
            }
        }
    }
    
    public Collection<Map.Entry<Key,Value>> getEventEntriesFromAccumulo(String queryId) {
        Collection<Map.Entry<Key,Value>> entries = new ArrayList<>();
        try {
            String indexTable = this.queryMetricHandlerProperties.getIndexTableName();
            String shardTable = this.queryMetricHandlerProperties.getShardTableName();
            Collection<Map.Entry<Key,Value>> indexEntries = getAccumuloEntries(this.accumuloClient, indexTable, this.auths);
            Key key = null;
            Value value = null;
            for (Map.Entry<Key,Value> e : indexEntries) {
                Key k = e.getKey();
                if (k.getRow().toString().equals(queryId) && k.getColumnFamily().toString().equals("QUERY_ID")) {
                    key = k;
                    value = e.getValue();
                    break;
                }
            }
            String uid = null;
            if (value != null) {
                Uid.List l = Uid.List.parseFrom(value.get());
                if (l.getUIDCount() > 0) {
                    uid = l.getUID(0);
                }
                if (uid != null) {
                    String[] split = key.getColumnQualifier().toString().split("\0");
                    if (split.length > 0) {
                        Text eventRow = new Text(split[0]);
                        Text eventColFam = new Text("querymetrics\0" + uid);
                        Collection<Map.Entry<Key,Value>> shardEntries = getAccumuloEntries(this.accumuloClient, shardTable, this.auths);
                        for (Map.Entry<Key,Value> e : shardEntries) {
                            if (e.getKey().getRow().equals(eventRow) && e.getKey().getColumnFamily().equals(eventColFam)) {
                                entries.add(e);
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return entries;
    }
    
    public void printEventEntriesFromAccumulo(String queryId) {
        Collection<Map.Entry<Key,Value>> entries = getEventEntriesFromAccumulo(queryId);
        for (Map.Entry<Key,Value> e : entries) {
            System.out.println(e.getKey().toString());
        }
    }
    
    public void assertNoDuplicateFields(String queryId) {
        Collection<Map.Entry<Key,Value>> entries = getEventEntriesFromAccumulo(queryId);
        Set<String> fields = new HashSet<>();
        Set<String> duplicateFields = new HashSet<>();
        for (Map.Entry<Key,Value> e : entries) {
            Key k = e.getKey();
            String[] split = k.getColumnQualifier().toString().split("\0");
            if (split.length > 0) {
                String f = split[0];
                if (!fields.contains(f)) {
                    fields.add(f);
                } else {
                    duplicateFields.add(f);
                }
            }
        }
        
        if (!duplicateFields.isEmpty()) {
            for (Map.Entry<Key,Value> e : entries) {
                System.out.println(e.getKey().toString());
            }
            fail("Duplicate field values found for:" + duplicateFields);
        }
    }
    
    @Configuration
    @Profile("MessageRouting")
    @ComponentScan(basePackages = "datawave.microservice")
    public static class MessagingTestConfiguration {
        @Primary
        @Bean
        public QueryMetricSupplier testQueryMetricSource(@Lazy QueryMetricOperations queryMetricOperations) {
            return new QueryMetricSupplier() {
                @Override
                public boolean send(Message<QueryMetricUpdate> queryMetricUpdate) {
                    queryMetricOperations.storeMetricUpdate(new QueryMetricUpdateHolder(queryMetricUpdate.getPayload()));
                    return true;
                }
            };
        }
    }
}
