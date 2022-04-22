package datawave.webservice.results.cached;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Preconditions;
import datawave.annotation.ClearQuerySessionId;
import datawave.annotation.GenerateQuerySessionId;
import datawave.annotation.Required;
import datawave.configuration.spring.SpringBean;
import datawave.interceptor.RequiredInterceptor;
import datawave.interceptor.ResponseInterceptor;
import datawave.marking.MarkingFunctions;
import datawave.marking.SecurityMarking;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.QueryMetric;
import datawave.microservice.querymetric.QueryMetricFactory;
import datawave.resteasy.interceptor.CreateQuerySessionIDFilter;
import datawave.security.authorization.DatawavePrincipal;
import datawave.webservice.common.audit.AuditBean;
import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.audit.Auditor.AuditType;
import datawave.webservice.common.audit.PrivateAuditConstants;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.common.exception.DatawaveWebApplicationException;
import datawave.webservice.common.exception.NoResultsException;
import datawave.webservice.common.exception.NotFoundException;
import datawave.webservice.common.exception.PreConditionFailedException;
import datawave.webservice.common.exception.QueryCanceledException;
import datawave.webservice.common.exception.UnauthorizedException;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryParameters;
import datawave.webservice.query.cache.CachedResultsQueryCache;
import datawave.webservice.query.cache.CreatedQueryLogicCacheBean;
import datawave.webservice.query.cache.QueryCache;
import datawave.webservice.query.cache.QueryExpirationConfiguration;
import datawave.webservice.query.cache.ResultsPage;
import datawave.webservice.query.cache.RunningQueryTimingImpl;
import datawave.webservice.query.cachedresults.CacheableLogic;
import datawave.webservice.query.cachedresults.CacheableQueryRow;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.NoResultsQueryException;
import datawave.webservice.query.exception.NotFoundQueryException;
import datawave.webservice.query.exception.PreConditionFailedQueryException;
import datawave.webservice.query.exception.QueryCanceledQueryException;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.exception.UnauthorizedQueryException;
import datawave.webservice.query.factory.Persister;
import datawave.webservice.query.logic.QueryLogic;
import datawave.webservice.query.logic.QueryLogicFactory;
import datawave.webservice.query.metric.QueryMetricsBean;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.query.runner.AccumuloConnectionRequestBean;
import datawave.webservice.query.runner.QueryPredictor;
import datawave.webservice.query.runner.RunningQuery;
import datawave.webservice.query.util.QueryUncaughtExceptionHandler;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.CachedResultsDescribeResponse;
import datawave.webservice.result.CachedResultsResponse;
import datawave.webservice.result.GenericResponse;
import datawave.webservice.result.TotalResultsAware;
import datawave.webservice.result.VoidResponse;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.trace.Span;
import org.apache.accumulo.core.trace.Trace;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.commons.collections4.Transformer;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.jexl2.parser.TokenMgrError;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.jboss.resteasy.annotations.GZIP;
import org.jboss.resteasy.specimpl.MultivaluedMapImpl;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.annotation.security.DeclareRoles;
import javax.annotation.security.RolesAllowed;
import javax.ejb.AsyncResult;
import javax.ejb.Asynchronous;
import javax.ejb.EJBContext;
import javax.ejb.EJBException;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.ejb.TransactionManagement;
import javax.ejb.TransactionManagementType;
import javax.enterprise.concurrent.ManagedExecutorService;
import javax.inject.Inject;
import javax.interceptor.Interceptors;
import javax.sql.DataSource;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.Principal;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.sql.Types;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Future;

/**
 * CachedResultsBean loads the results of a predefined query into a relational database (MySQL) so that the user can run SQL queries against the data, which
 * allows sorting, grouping, etc. When a user calls load(), this bean creates a table in the database that has the following columns:
 * 
 * user, queryId, eventId, datatype, row, colf, visibility, markings, and columns 1 .. N.
 * 
 * Since we are paging through the results from ACCUMULO we don't know all of the possible field names when creating the table. For right now, this bean will
 * only work with results from the event query logic (ShardQueryTable). After the results have been loaded, a view will be created on the table that user
 * queries will run against. This view will look like:
 * 
 * user, queryId, eventId, datatype, row, colf, visibility, markings, field1, field2, field3, ...
 * 
 * Currently event attributes that have multiple values will be stored as a comma-separated string in MySQL. We could break these out into different rows in the
 * database and use the group_concat() SQL function to concatentate them at query time. Also, since the data is coming from a schema-less source, all columns in
 * the table will be of type Text.
 * 
 * Object that loads a predefined query into a relational database so that SQL queries can be run against it. Typical use case for this object is:
 *
 * load() create() repeated calls to next() or previous() close()
 *
 * The object is reusable, so the user could call setQuery again and run a different query against the result set.
 *
 */
@javax.ws.rs.Path("/CachedResults")
@RolesAllowed({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator", "JBossAdministrator"})
@DeclareRoles({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator", "JBossAdministrator"})
@Stateless
@LocalBean
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
@TransactionManagement(TransactionManagementType.BEAN)
public class CachedResultsBean {
    
    private static Logger log = Logger.getLogger(CachedResultsBean.class);
    
    @Resource
    private EJBContext ctx;
    
    @Inject
    private Persister persister;
    
    @Inject
    private AccumuloConnectionFactory connectionFactory;
    
    @Inject
    private QueryLogicFactory queryFactory;
    
    @Inject
    private QueryMetricsBean metrics;
    
    @Resource(lookup = "java:jboss/datasources/CachedResultsDS")
    protected DataSource ds;
    
    @Inject
    private QueryCache runningQueryCache;
    
    @Inject
    private AuditBean auditor;
    
    @Inject
    private CreatedQueryLogicCacheBean qlCache;
    
    @Resource
    private ManagedExecutorService executor;
    
    @Inject
    private QueryPredictor predictor;
    
    protected static final String BASE_COLUMNS = StringUtils.join(CacheableQueryRow.getFixedColumnSet(), ",");
    
    @Inject
    private ResponseObjectFactory responseObjectFactory;
    
    // reference "datawave/query/CachedResults.xml"
    @Inject
    @SpringBean(required = false, refreshable = true)
    private CachedResultsConfiguration cachedResultsConfiguration;
    
    @Inject
    private CachedResultsQueryCache cachedRunningQueryCache;
    
    @Inject
    private SecurityMarking marking;
    
    @Inject
    @SpringBean(refreshable = true)
    private QueryExpirationConfiguration queryExpirationConf;
    
    @Inject
    private QueryMetricFactory metricFactory;
    
    @Inject
    private AccumuloConnectionRequestBean accumuloConnectionRequestBean;
    
    protected static final String COMMA = ",";
    protected static final String TABLE = "$table";
    protected static final String FIELD = "field";
    
    private static final String IMPORT_FILE = "replication_scripts/import.sh";
    
    private static Map<String,RunningQuery> loadingQueryMap = Collections.synchronizedMap(new HashMap<>());
    private static Set<String> loadingQueries = Collections.synchronizedSet(new HashSet<>());
    private URL importFileUrl = null;
    private CachedResultsParameters cp = new CachedResultsParameters();
    
    @PostConstruct
    public void init() {
        // create the template table in the database if it does not exist.
        
        if (cachedResultsConfiguration.getDefaultPageSize() > cachedResultsConfiguration.getMaxPageSize()) {
            throw new EJBException("The default page size " + cachedResultsConfiguration.getDefaultPageSize() + " has been set larger than the max page size "
                            + cachedResultsConfiguration.getMaxPageSize());
        }
        
        try {
            importFileUrl = new File(System.getProperty("jboss.home.dir"), IMPORT_FILE).toURI().toURL();
            log.info("import.sh: " + importFileUrl);
        } catch (MalformedURLException e) {
            log.error("Error getting import.sh", e);
            importFileUrl = null;
        }
        
        CachedRunningQuery.setDatasource(ds);
        CachedRunningQuery.setQueryFactory(queryFactory);
        CachedRunningQuery.setResponseObjectFactory(responseObjectFactory);
        
        String template = null;
        try (Connection con = ds.getConnection(); Statement s = con.createStatement()) {
            template = cachedResultsConfiguration.getParameters().get("TEMPLATE_TABLE");
            s.execute(template);
        } catch (SQLException sqle) {
            log.error(sqle.getMessage(), sqle);
            throw new EJBException("Unable to create template table with statement: " + template, sqle);
        }
    }
    
    protected void loadBatch(PreparedStatement ps, String owner, String queryId, String logicName, Map<String,Integer> fieldMap, CacheableQueryRow cqo,
                    int maxFieldSize) throws SQLException {
        
        // Maintain a list of the columns that are populated so
        // that we can
        // set the others to null.
        HashSet<Integer> populatedColumns = new HashSet<>();
        // Done capturing all the fields in the event, insert
        // into database.
        ps.clearParameters(); // not sure we need this
        
        // Each entry is a different visibility.
        ps.setString(1, owner);
        ps.setString(2, queryId);
        ps.setString(3, logicName);
        ps.setString(4, cqo.getDataType());
        ps.setString(5, cqo.getEventId());
        ps.setString(6, cqo.getRow());
        ps.setString(7, cqo.getColFam());
        ps.setString(8, MarkingFunctions.Encoding.toString(new TreeMap<>(cqo.getMarkings())));
        for (Entry<String,String> e : cqo.getColumnValues().entrySet()) {
            
            String columnName = e.getKey();
            String columnValue = e.getValue();
            // Get the field number from the fieldMap.
            Integer columnNumber = fieldMap.get(columnName);
            if (columnNumber == null) {
                columnNumber = CacheableQueryRow.getFixedColumnSet().size() + fieldMap.size() + 1;
                fieldMap.put(columnName, columnNumber);
            }
            
            if (columnValue.length() > maxFieldSize) {
                columnValue = columnValue.substring(0, maxFieldSize) + "<truncated>";
                ps.setString(columnNumber, columnValue);
            } else {
                ps.setString(columnNumber, columnValue);
            }
            populatedColumns.add(columnNumber);
            if (log.isTraceEnabled()) {
                log.trace("Set parameter: " + columnNumber + " with field name: " + columnName + " to value: " + columnValue);
            }
        }
        
        ps.setString(9, cqo.getColumnSecurityMarkingString(fieldMap));
        ps.setString(10, cqo.getColumnTimestampString(fieldMap));
        
        // Need to set all of the remaining parameters to
        // NULL
        int startCol = CacheableQueryRow.getFixedColumnSet().size() + 1;
        int maxCol = CacheableQueryRow.getFixedColumnSet().size() + 901;
        for (int i = startCol; i < maxCol; i++) {
            if (!populatedColumns.contains(i)) {
                ps.setNull(i, Types.VARCHAR);
            }
        }
        ps.addBatch();
        
    }
    
    protected GenericResponse<String> load(@Required("queryId") String queryId, String alias, String nameBase) {
        
        GenericResponse<String> response = new GenericResponse<>();
        
        // Find out who/what called this method
        Principal p = ctx.getCallerPrincipal();
        String owner = getOwnerFromPrincipal(p);
        String userDn = getDNFromPrincipal(p);
        Collection<Collection<String>> cbAuths = new HashSet<>();
        if (p instanceof DatawavePrincipal) {
            DatawavePrincipal dp = (DatawavePrincipal) p;
            cbAuths.addAll(dp.getAuthorizations());
        } else {
            QueryException qe = new QueryException(DatawaveErrorCode.UNEXPECTED_PRINCIPAL_ERROR, MessageFormat.format("Class: {0}", p.getClass().getName()));
            response.addException(qe);
            throw new DatawaveWebApplicationException(qe, response);
        }
        
        AccumuloConnectionFactory.Priority priority;
        Connector connector = null;
        RunningQuery query = null;
        String tableName = "t" + nameBase;
        String viewName = "v" + nameBase;
        Connection con = null;
        PreparedStatement ps = null;
        boolean tableCreated = false;
        boolean viewCreated = false;
        CachedRunningQuery crq = null;
        Span span = null;
        boolean queryLockedException = false;
        int rowsPerBatch = cachedResultsConfiguration.getRowsPerBatch();
        try {
            
            // This RunningQuery may be in use. Make a copy using the defined Query.
            
            RunningQuery rq = null;
            QueryLogic<?> logic = null;
            Query q = null;
            BaseQueryMetric queryMetric = null;
            TInfo traceInfo = null;
            try {
                rq = getQueryById(queryId);
                
                // prevent duplicate calls to load with the same queryId
                if (CachedResultsBean.loadingQueries.contains(queryId)) {
                    // if a different thread is using rq, we don't want to modify it in the finally block
                    rq = null;
                    // this is used in the inside & outside finally block to bypass cleanup that would adversely affect the loading query
                    queryLockedException = true;
                    throw new QueryException(DatawaveErrorCode.QUERY_LOCKED_ERROR);
                } else {
                    CachedResultsBean.loadingQueries.add(queryId);
                }
                
                rq.setActiveCall(true);
                Query originalQuery = rq.getSettings();
                q = originalQuery.duplicate(originalQuery.getQueryName());
                q.setId(originalQuery.getId());
                q.setUncaughtExceptionHandler(new QueryUncaughtExceptionHandler());
                Thread.currentThread().setUncaughtExceptionHandler(q.getUncaughtExceptionHandler());
                
                queryMetric = rq.getMetric().duplicate();
                // clear page times
                queryMetric.setPageTimes(new ArrayList<>());
                // will throw IllegalArgumentException if not defined
                logic = rq.getLogic();
                // need to clone the logic here because the QueryExpirationBean will call close on
                // rq and RunningQuery.close will call close on the logic. This is causing the batch scanner to
                // be closed after 15 minutes
                logic = (QueryLogic<?>) logic.clone();
                if (rq.getTraceInfo() != null) {
                    traceInfo = rq.getTraceInfo().deepCopy();
                }
            } finally {
                if (rq != null) {
                    // the original query was cloned including the queryId
                    // remove original query from the cache to avoid duplicate metrics
                    // when it is expired by the QueryExpirationBean
                    rq.setActiveCall(false);
                    if (rq.getConnection() != null) {
                        connectionFactory.returnConnection(rq.getConnection());
                    }
                    runningQueryCache.remove(queryId);
                }
            }
            
            try {
                persistByQueryId(viewName, alias, owner, CachedRunningQuery.Status.LOADING, "", false);
            } catch (IOException e2) {
                PreConditionFailedQueryException e = new PreConditionFailedQueryException(DatawaveErrorCode.CACHED_RESULTS_IMPORT_ERROR, e2);
                response.addException(e);
                response.setResult("Error loading results into cache");
                throw new PreConditionFailedException(e, response);
            }
            
            // Get a accumulo connection
            priority = logic.getConnectionPriority();
            Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            addQueryToTrackingMap(trackingMap, q);
            accumuloConnectionRequestBean.requestBegin(queryId);
            try {
                connector = connectionFactory.getConnection(priority, trackingMap);
            } finally {
                accumuloConnectionRequestBean.requestEnd(queryId);
            }
            
            CacheableLogic cacheableLogic;
            Transformer t = logic.getTransformer(q);
            
            // Audit the query. This may be duplicative if the caller called
            // QueryExecutorBean.create() or QueryExecutorBean.reset() first.
            AuditType auditType = logic.getAuditType(q);
            if (!auditType.equals(AuditType.NONE)) {
                try {
                    MultivaluedMap<String,String> queryMap = new MultivaluedMapImpl<>();
                    queryMap.putAll(q.toMap());
                    marking.validate(queryMap);
                    queryMap.putSingle(PrivateAuditConstants.COLUMN_VISIBILITY, marking.toColumnVisibilityString());
                    queryMap.putSingle(PrivateAuditConstants.AUDIT_TYPE, auditType.name());
                    queryMap.putSingle(PrivateAuditConstants.USER_DN, q.getUserDN());
                    queryMap.putSingle(PrivateAuditConstants.LOGIC_CLASS, logic.getLogicName());
                    try {
                        List<String> selectors = logic.getSelectors(q);
                        if (selectors != null && !selectors.isEmpty()) {
                            queryMap.put(PrivateAuditConstants.SELECTORS, selectors);
                        }
                    } catch (Exception e) {
                        log.error(e.getMessage());
                    }
                    // if the user didn't set an audit id, use the query id
                    if (!queryMap.containsKey(AuditParameters.AUDIT_ID)) {
                        queryMap.putSingle(AuditParameters.AUDIT_ID, q.getId().toString());
                    }
                    auditor.audit(queryMap);
                } catch (Exception e) {
                    QueryException qe = new QueryException(DatawaveErrorCode.QUERY_AUDITING_ERROR, e);
                    log.error(qe);
                    response.addException(qe.getBottomQueryException());
                    throw new DatawaveWebApplicationException(qe, response);
                }
            }
            
            if (t instanceof CacheableLogic) {
                // hold on to a reference of the query logic so we cancel it if need be.
                qlCache.add(q.getId().toString(), owner, logic, connector);
                
                try {
                    query = new RunningQuery(null, null, logic.getConnectionPriority(), logic, q, q.getQueryAuthorizations(), p, new RunningQueryTimingImpl(
                                    queryExpirationConf, q.getPageTimeout()), executor, predictor, metricFactory);
                    query.setActiveCall(true);
                    // queryMetric was duplicated from the original earlier
                    query.setMetric(queryMetric);
                    query.setQueryMetrics(metrics);
                    query.setConnection(connector);
                    // Copy trace info from a clone of the original query
                    query.setTraceInfo(traceInfo);
                } finally {
                    qlCache.poll(q.getId().toString());
                }
                
                cacheableLogic = (CacheableLogic) t;
                CachedResultsBean.loadingQueryMap.put(queryId, query);
            } else {
                throw new IllegalArgumentException(logic.getLogicName() + " does not support CachedResults calls");
            }
            
            try {
                con = ds.getConnection();
                // Create the result table for this query
                Statement s = con.createStatement();
                String createTable = cachedResultsConfiguration.getParameters().get("CREATE_TABLE");
                createTable = createTable.replace(TABLE, tableName);
                s.execute(createTable);
                s.close();
                tableCreated = true;
                // Parse the PreparedStatement
                String insert = cachedResultsConfiguration.getParameters().get("INSERT");
                insert = insert.replace(TABLE, tableName);
                ps = con.prepareStatement(insert);
            } catch (SQLException sqle) {
                throw new QueryException(DatawaveErrorCode.CACHED_RESULTS_TABLE_CREATE_ERROR, sqle);
            }
            
            // Object for keeping track of which fields are placed in which
            // table columns
            // Key is fieldName, value is column number
            Map<String,Integer> fieldMap = new HashMap<>();
            
            // Loop over the results and put them into the database.
            ResultsPage results = null;
            
            // If we're tracing this query, then continue the trace for the next call.
            if (traceInfo != null) {
                span = Trace.trace(traceInfo, "cachedresults:load");
            }
            
            int rowsWritten = 0;
            boolean go = true;
            while (go) {
                
                if (query.isCanceled()) {
                    throw new QueryCanceledQueryException(DatawaveErrorCode.QUERY_CANCELED);
                }
                
                Span nextSpan = (span == null) ? null : Trace.start("cachedresults:next");
                try {
                    if (nextSpan != null)
                        nextSpan.data("pageNumber", Long.toString(query.getLastPageNumber() + 1));
                    
                    results = query.next();
                } finally {
                    if (nextSpan != null)
                        nextSpan.stop();
                }
                if (results.getResults().isEmpty()) {
                    go = false;
                    break;
                }
                
                int maxLength = 0;
                for (Object o : results.getResults()) {
                    
                    List<CacheableQueryRow> cacheableQueryRowList = cacheableLogic.writeToCache(o);
                    
                    for (CacheableQueryRow cacheableQueryObject : cacheableQueryRowList) {
                        
                        Collection<String> values = ((CacheableQueryRow) cacheableQueryObject).getColumnValues().values();
                        int maxValueLength = 0;
                        for (String s : values) {
                            if (s.length() > maxValueLength) {
                                maxValueLength = s.length();
                            }
                        }
                        
                        boolean dataWritten = false;
                        // If a successful maxLength has been determined, then don't change it.
                        if (maxLength == 0)
                            maxLength = maxValueLength + 1;
                        else if (maxValueLength > maxLength) {
                            maxLength = maxValueLength;
                        }
                        
                        int attempt = 0;
                        SQLException loadBatchException = null; // exception;
                        while (dataWritten == false && attempt < 10) {
                            try {
                                loadBatch(ps, owner, queryId, logic.getLogicName(), fieldMap, cacheableQueryObject, maxLength);
                                dataWritten = true;
                                rowsWritten++;
                            } catch (SQLException e) {
                                loadBatchException = e;
                                String msg = e.getMessage();
                                if (msg.startsWith("Table") && msg.endsWith("doesn't exist")) {
                                    throw new QueryException(DatawaveErrorCode.CACHE_TABLE_MISSING, MessageFormat.format("message: {0}", msg));
                                } else {
                                    log.info("Caught other SQLException:" + msg + " writing batch with maxLength:" + maxLength);
                                    maxLength = maxLength / 2;
                                }
                            }
                            attempt++;
                        }
                        
                        if (dataWritten == false) {
                            String message = (loadBatchException == null) ? "unknown" : loadBatchException.getMessage();
                            
                            log.error("Batch write FAILED - last exception = " + message + "record = " + cacheableQueryObject.getColumnValues().entrySet(),
                                            loadBatchException);
                        } else if (rowsWritten >= rowsPerBatch) {
                            persistBatch(ps);
                            ps.clearBatch();
                            rowsWritten = 0;
                        }
                    }
                }
            } // End of inserts into table
            
            // commit the last batch
            if (rowsWritten > 0) {
                persistBatch(ps);
                ps.clearBatch();
                rowsWritten = 0;
            }
            
            // Dump the fieldMap for debugging
            if (log.isTraceEnabled()) {
                for (Entry<String,Integer> e : fieldMap.entrySet()) {
                    log.trace("Field mapping: " + e.getKey() + " -> " + e.getValue());
                }
            }
            
            // Create the view of the table
            viewCreated = createView(tableName, viewName, con, viewCreated, fieldMap);
            
            // create the CachedRunningQuery and store it under the originalQueryName, but do not activate it
            crq = new CachedRunningQuery(q, logic, viewName, alias, owner, viewName, cachedResultsConfiguration.getDefaultPageSize(), queryId,
                            fieldMap.keySet(), null, metricFactory);
            crq.setOriginalQueryId(queryId);
            crq.setTableName(tableName);
            crq.setStatus(CachedRunningQuery.Status.LOADED);
            crq.setPrincipal(ctx.getCallerPrincipal());
            persist(crq, owner);
            
            crq.getMetric().setLifecycle(QueryMetric.Lifecycle.INITIALIZED);
            
            response.setResult(viewName);
            if (fieldMap.isEmpty()) {
                throw new NoResultsQueryException("Field map is empty.", "204-4");
            } else {
                return response;
            }
        } catch (NoResultsQueryException e) {
            crq.getMetric().setLifecycle(QueryMetric.Lifecycle.DEFINED);
            try {
                persistByQueryId(viewName, alias, owner, CachedRunningQuery.Status.LOADED, "", false);
            } catch (IOException e1) {
                response.addException(new PreConditionFailedQueryException(DatawaveErrorCode.CACHED_RESULTS_IMPORT_ERROR, e1));
            }
            response.addException(e.getBottomQueryException());
            throw new NoResultsException(e, response.getResult());
        } catch (QueryCanceledQueryException | InterruptedException e) {
            log.info("Query " + queryId + " canceled on request");
            if (crq != null) {
                crq.getMetric().setLifecycle(QueryMetric.Lifecycle.CANCELLED);
            }
            try {
                persistByQueryId(viewName, alias, owner, CachedRunningQuery.Status.CANCELED, "query canceled", false);
            } catch (IOException e1) {
                response.addException(new PreConditionFailedQueryException(DatawaveErrorCode.CACHED_RESULTS_IMPORT_ERROR, e1));
            }
            QueryException qe = new QueryException(DatawaveErrorCode.QUERY_CANCELED, e);
            response.addException(qe.getBottomQueryException());
            throw new QueryCanceledException(qe, response);
        } catch (Throwable t) {
            if (crq != null && crq.getQueryLogic().getCollectQueryMetrics() == true) {
                try {
                    crq.getMetric().setError(t);
                    crq.getMetric().setLifecycle(QueryMetric.Lifecycle.DEFINED);
                    metrics.updateMetric(crq.getMetric());
                } catch (Exception e1) {
                    log.error(e1.getMessage(), e1);
                }
            }
            String statusMessage = t.getMessage();
            if (null == statusMessage) {
                statusMessage = t.getClass().getName();
            }
            
            try {
                persistByQueryId(viewName, alias, owner, CachedRunningQuery.Status.ERROR, statusMessage, false);
            } catch (IOException e2) {
                response.addException(new PreConditionFailedQueryException(DatawaveErrorCode.CACHED_RESULTS_IMPORT_ERROR, e2));
            }
            // don't log stack trace of parse errors and other IllegalArgumentExceptions
            if (t instanceof IllegalArgumentException || t instanceof TokenMgrError) {
                log.info(t.getMessage());
            } else {
                log.error(t.getMessage(), t);
            }
            if (con != null) {
                Statement s = null;
                try {
                    s = con.createStatement();
                    if (tableCreated) {
                        // Drop the result table and view for this query
                        String dropTable = cachedResultsConfiguration.getParameters().get("DROP_TABLE");
                        dropTable = dropTable.replace(TABLE, tableName);
                        s.execute(dropTable);
                    }
                    if (viewCreated) {
                        String dropView = cachedResultsConfiguration.getParameters().get("DROP_VIEW");
                        dropView = dropView.replace(TABLE, viewName);
                        s.execute(dropView);
                    }
                    s.close();
                } catch (Exception e1) {
                    log.error(e1.getMessage(), e1);
                    response.addException(new QueryException(DatawaveErrorCode.FAILURE_CLEANUP_ERROR, e1).getBottomQueryException());
                } finally {
                    DbUtils.closeQuietly(s);
                }
            }
            if (t instanceof Error && (t instanceof TokenMgrError) == false) {
                throw (Error) t;
            }
            
            // default status code
            int statusCode = Response.Status.INTERNAL_SERVER_ERROR.getStatusCode();
            if (t instanceof QueryException) {
                response.addException(((QueryException) t).getBottomQueryException());
                statusCode = ((QueryException) t).getBottomQueryException().getStatusCode();
            } else {
                QueryException qe = new QueryException(statusMessage, t);
                response.addException(qe);
            }
            throw new DatawaveWebApplicationException(t, response, statusCode);
        } finally {
            DbUtils.closeQuietly(con, ps, null);
            if (queryLockedException == false) {
                CachedResultsBean.loadingQueryMap.remove(queryId);
                CachedResultsBean.loadingQueries.remove(queryId);
            }
            
            if (span != null) {
                span.stop();
                
                span = Trace.trace(query.getTraceInfo(), "query:close");
                span.data("closedAt", new Date().toString());
                // Spans aren't recorded if they take no time, so sleep for a
                // couple milliseconds just to ensure we get something saved.
                try {
                    Thread.sleep(2);
                } catch (InterruptedException e) {
                    // ignore
                }
                span.stop();
                // TODO: 1.8.1: no longer done?
                // Tracer.getInstance().flush();
            }
            
            if (null != query) {
                query.setActiveCall(false);
                try {
                    query.closeConnection(connectionFactory);
                } catch (Exception e) {
                    response.addException(new QueryException(DatawaveErrorCode.QUERY_CLOSE_ERROR, e).getBottomQueryException());
                }
            } else if (connector != null) {
                try {
                    connectionFactory.returnConnection(connector);
                } catch (Exception e) {
                    log.error(new QueryException(DatawaveErrorCode.CONNECTOR_RETURN_ERROR, e));
                }
            }
        }
    }
    
    /**
     * Returns status of the requested cached result
     *
     * @param queryId
     * @return List of attribute names that can be used in subsequent queries
     *
     * @return {@code datawave.webservice.result.GenericResponse<String>}
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     *
     * @HTTP 200 success
     * @HTTP 404 not found
     * @HTTP 412 not yet loaded
     * @HTTP 500 internal server error
     *
     */
    @GET
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf"})
    @javax.ws.rs.Path("/{queryId}/status")
    @GZIP
    @Interceptors({RequiredInterceptor.class, ResponseInterceptor.class})
    @Timed(name = "dw.cachedr.status", absolute = true)
    public GenericResponse<String> status(@PathParam("queryId") @Required("queryId") String queryId) {
        
        GenericResponse<String> response = new GenericResponse<>();
        
        // Find out who/what called this method
        Principal p = ctx.getCallerPrincipal();
        
        String owner = getOwnerFromPrincipal(p);
        CachedRunningQuery crq;
        try {
            crq = retrieve(queryId, owner);
        } catch (IOException e1) {
            PreConditionFailedQueryException e = new PreConditionFailedQueryException(DatawaveErrorCode.CACHED_RESULTS_IMPORT_ERROR, e1);
            response.addException(e);
            response.setResult("CachedResult not found");
            throw new PreConditionFailedException(e, response);
        }
        
        if (null == crq) {
            NotFoundQueryException e = new NotFoundQueryException(DatawaveErrorCode.CACHED_RESULT_NOT_FOUND);
            response.addException(e);
            response.setResult("CachedResult not found");
            throw new NotFoundException(e, response);
        }
        
        if (!crq.getUser().equals(owner)) {
            UnauthorizedQueryException e = new UnauthorizedQueryException(DatawaveErrorCode.QUERY_OWNER_MISMATCH, MessageFormat.format("{0} != {1}",
                            crq.getUser(), owner));
            response.addException(e);
            response.setResult("Current user does not match user that defined query.");
            throw new UnauthorizedException(e, response);
        }
        
        CachedRunningQuery.Status status = crq.getStatus();
        
        if (status == null) {
            response.setResult(CachedRunningQuery.Status.NONE.toString());
        } else {
            response.setResult(status.toString());
        }
        
        if (crq.getStatusMessage() != null && crq.getStatusMessage().isEmpty() == false) {
            response.addMessage(crq.getStatusMessage());
        }
        
        return response;
    }
    
    private String getOwnerFromPrincipal(Principal p) {
        String owner = p.getName();
        if (p instanceof DatawavePrincipal) {
            DatawavePrincipal cp = (DatawavePrincipal) p;
            owner = cp.getShortName();
        }
        return owner;
    }
    
    private String getDNFromPrincipal(Principal p) {
        String dn = p.getName();
        if (p instanceof DatawavePrincipal) {
            DatawavePrincipal cp = (DatawavePrincipal) p;
            dn = cp.getUserDN().subjectDN();
        }
        return dn;
    }
    
    protected void persistBatch(PreparedStatement ps) throws SQLException {
        int[] batchResults = null;
        try {
            batchResults = ps.executeBatch();
            int failCount = 0;
            for (int i = 0; i < batchResults.length; i++) {
                if (batchResults[i] == Statement.EXECUTE_FAILED) {
                    failCount++;
                }
            }
            
            if (failCount > 0) {
                StringBuilder b = new StringBuilder();
                for (int i = 0; i < batchResults.length; i++) {
                    if (batchResults[i] == Statement.EXECUTE_FAILED) {
                        b.append(i).append(" ");
                    }
                }
                log.warn("Batch failed to perform " + failCount + " updates, indexes: " + b);
            } else if (log.isDebugEnabled()) {
                log.debug("Successfully persisted batch of size: " + batchResults.length + " total " + failCount + " failures");
            }
        } catch (BatchUpdateException be) {
            log.warn("Caught BatchUpdateException, one or more batch update have failed: " + be.getMessage(), be);
            throw be;
        } catch (SQLException sqle) {
            log.error("Error committing last batch", sqle);
            throw sqle;
        }
    }
    
    /**
     * Loads the results of the defined query, specified by query id, into a store that allows SQL queries to be run against it. This allows caller to sort and
     * group by attributes
     *
     * @param queryId
     *            - queryId that identifies the original query that the user wants cached (required)
     * @param alias
     *            additional name that this query can be retrieved by
     * @return name of the view for this query, use it as the table name in the SQL query
     *
     * @return {@code datawave.webservice.result.GenericResponse<String>}
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     * @ResponseHeader query-session-id this header and value will be in the Set-Cookie header, subsequent calls for this session will need to supply the
     *                 query-session-id header in the request in a Cookie header or as a query parameter
     *
     * @HTTP 200 success
     * @HTTP 500 internal server error
     */
    @GET
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml"})
    @javax.ws.rs.Path("/load")
    @GenerateQuerySessionId(cookieBasePath = "/DataWave/CachedResults/")
    @Interceptors({RequiredInterceptor.class, ResponseInterceptor.class})
    public GenericResponse<String> load(@QueryParam("queryId") @Required("queryId") String queryId, @QueryParam("alias") String alias) {
        
        String nameBase = UUID.randomUUID().toString().replaceAll("-", "");
        CreateQuerySessionIDFilter.QUERY_ID.set(queryId);
        return load(queryId, alias, nameBase);
    }
    
    @GET
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml"})
    @javax.ws.rs.Path("/async/load")
    @GenerateQuerySessionId(cookieBasePath = "/DataWave/CachedResults/")
    @Interceptors({RequiredInterceptor.class, ResponseInterceptor.class})
    @Asynchronous
    public void loadAsync(@QueryParam("queryId") @Required("queryId") String queryId, @QueryParam("alias") String alias, @Suspended AsyncResponse asyncResponse) {
        
        String nameBase = UUID.randomUUID().toString().replaceAll("-", "");
        CreateQuerySessionIDFilter.QUERY_ID.set(queryId);
        try {
            GenericResponse<String> response = load(queryId, alias, nameBase);
            asyncResponse.resume(response);
        } catch (Throwable t) {
            asyncResponse.resume(t);
        }
    }
    
    /**
     *
     * @param queryParameters
     *
     * @return datawave.webservice.result.CachedResultsResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @ResponseHeader query-session-id this header and value will be in the Set-Cookie header, subsequent calls for this session will need to supply the
     *                 query-session-id header in the request in a Cookie header or as a query parameter
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     *
     * @HTTP 200 success
     * @HTTP 500 internal server error
     *
     */
    @POST
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml"})
    @javax.ws.rs.Path("/{queryId}/loadAndCreate")
    @Interceptors(RequiredInterceptor.class)
    @GenerateQuerySessionId(cookieBasePath = "/DataWave/CachedResults/")
    @Timed(name = "dw.cachedr.loadAndCreate", absolute = true)
    public CachedResultsResponse loadAndCreate(@Required("queryId") @PathParam("queryId") String queryId, MultivaluedMap<String,String> queryParameters) {
        CreateQuerySessionIDFilter.QUERY_ID.set(null);
        
        String newQueryId = queryParameters.getFirst("newQueryId");
        Preconditions.checkNotNull(newQueryId, "newQueryId cannot be null");
        
        Preconditions.checkNotNull(queryId, "queryId cannot be null");
        queryParameters.putSingle(CachedResultsParameters.QUERY_ID, queryId);
        
        String alias = queryParameters.getFirst(CachedResultsParameters.ALIAS);
        
        // Find out who/what called this method
        Principal p = ctx.getCallerPrincipal();
        String owner = getOwnerFromPrincipal(p);
        
        GenericResponse<String> r = null;
        try {
            r = load(queryId, alias);
        } catch (DatawaveWebApplicationException e) {
            if (e instanceof NoResultsException == false) {
                
                if (e.getCause() instanceof QueryCanceledException) {
                    try {
                        persistByQueryId(newQueryId, alias, owner, CachedRunningQuery.Status.CANCELED, "query canceled", true);
                    } catch (IOException e1) {
                        log.error("Error persisting state to CachedResults store", e1);
                    }
                    throw e;
                } else {
                    String statusMessage = e.getCause().getMessage();
                    if (null == statusMessage) {
                        statusMessage = e.getClass().getName();
                    }
                    try {
                        persistByQueryId(newQueryId, alias, owner, CachedRunningQuery.Status.ERROR, statusMessage, true);
                    } catch (IOException e1) {
                        log.error("Error persisting state to CachedResults store", e1);
                    }
                    throw e;
                }
            } else if (e.getResponse().getEntity() == null) {
                // NoResultsException can't contain the response object, otherwise we'll return invalid HTML. So, instead, pull the ID from the
                // NoResultsException and make a new GenericResponse here.
                r = new GenericResponse<>();
                r.setResult(((NoResultsException) e).getId());
            }
            if (e.getResponse().getEntity() instanceof GenericResponse<?>) {
                @SuppressWarnings("unchecked")
                GenericResponse<String> gr = (GenericResponse<String>) e.getResponse().getEntity();
                r = gr;
            } else if (r == null) {
                throw e;
            }
        } catch (RuntimeException e) {
            log.error(e.getMessage(), e);
            throw e;
        }
        String view = r.getResult();
        
        // pagesize validated in create
        CreateQuerySessionIDFilter.QUERY_ID.set(newQueryId);
        queryParameters.remove(CachedResultsParameters.QUERY_ID);
        queryParameters.remove(CachedResultsParameters.VIEW);
        queryParameters.putSingle(CachedResultsParameters.VIEW, view);
        CachedResultsResponse response = create(newQueryId, queryParameters);
        try {
            persistByQueryId(newQueryId, alias, owner, CachedRunningQuery.Status.AVAILABLE, "", true);
        } catch (IOException e) {
            QueryException qe = new QueryException(DatawaveErrorCode.CACHE_PERSISTANCE_ERROR, e);
            response.addException(qe);
            throw new DatawaveWebApplicationException(e, response);
        }
        return response;
    }
    
    // Do not use the @Asynchronous annotation here. This method runs (calling the other version), setting
    // status and then executes loadAndCreate asynchronously. It does not itself get run asynchronously.
    @Interceptors({RequiredInterceptor.class, ResponseInterceptor.class})
    public Future<CachedResultsResponse> loadAndCreateAsync(@Required("newQueryId") String newQueryId, String alias, @Required("queryId") String queryId,
                    @Required("fields") String fields, String conditions, String grouping, String order, @Required("columnVisibility") String columnVisibility,
                    @DefaultValue("-1") Integer pagesize, String fixedFieldsInEvent) {
        
        MultivaluedMap<String,String> queryParameters = new MultivaluedMapImpl<>();
        queryParameters.putSingle(CachedResultsParameters.QUERY_ID, queryId);
        queryParameters.putSingle("newQueryId", newQueryId);
        queryParameters.putSingle(CachedResultsParameters.ALIAS, alias);
        queryParameters.putSingle(CachedResultsParameters.FIELDS, fields);
        queryParameters.putSingle(CachedResultsParameters.CONDITIONS, conditions);
        queryParameters.putSingle(CachedResultsParameters.GROUPING, grouping);
        queryParameters.putSingle(CachedResultsParameters.ORDER, order);
        queryParameters.putSingle("columnVisibility", columnVisibility);
        queryParameters.putSingle(QueryParameters.QUERY_PAGESIZE, Integer.toString(pagesize));
        queryParameters.putSingle(CachedResultsParameters.FIXED_FIELDS_IN_EVENT, fixedFieldsInEvent);
        
        return loadAndCreateAsync(queryParameters);
    }
    
    // Do not use the @Asynchronous annotation here. This method runs, setting status and
    // then executes loadAndCreate asynchronously. It does not itself get run asynchronously.
    @Interceptors({RequiredInterceptor.class, ResponseInterceptor.class})
    public Future<CachedResultsResponse> loadAndCreateAsync(MultivaluedMap<String,String> queryParameters) {
        
        String newQueryId = queryParameters.getFirst("newQueryId");
        Preconditions.checkNotNull(newQueryId, "newQueryId cannot be null");
        
        String queryId = queryParameters.getFirst(CachedResultsParameters.QUERY_ID);
        Preconditions.checkNotNull(queryId, "queryId cannot be null");
        
        String alias = queryParameters.getFirst("alias");
        if (alias == null) {
            alias = newQueryId;
        }
        
        // Find out who/what called this method
        Principal p = ctx.getCallerPrincipal();
        String owner = getOwnerFromPrincipal(p);
        
        CachedRunningQuery crq = null;
        try {
            persistByQueryId(newQueryId, alias, owner, CachedRunningQuery.Status.LOADING, "", true);
            crq = retrieve(newQueryId, owner);
        } catch (IOException e1) {
            log.error("Error trying to persist/retrieve cached results", e1);
            try {
                persistByQueryId(newQueryId, alias, owner, CachedRunningQuery.Status.ERROR, e1.getMessage(), false);
            } catch (IOException e2) {
                log.error(e2.getMessage(), e2);
            }
            PreConditionFailedQueryException e = new PreConditionFailedQueryException(DatawaveErrorCode.CACHED_RESULTS_PERSIST_ERROR, e1);
            throw new PreConditionFailedException(e, null);
        }
        
        RunningQuery rq = null;
        try {
            rq = getQueryById(queryId);
            if (rq != null) {
                String nameBase = UUID.randomUUID().toString().replaceAll("-", "");
                Query q = rq.getSettings();
                crq.setOriginalQueryId(q.getId().toString());
                crq.setView(nameBase);
                crq.setAlias(alias);
                crq.setQuery(rq.getSettings());
                persist(crq, owner);
            }
            
        } catch (Exception e) {
            String statusMessage = e.getMessage();
            if (null == statusMessage) {
                statusMessage = e.getClass().getName();
            }
            
            try {
                persistByQueryId(newQueryId, alias, owner, CachedRunningQuery.Status.ERROR, statusMessage, true);
            } catch (IOException e1) {
                log.error("Error persisting status to CachedResult store", e1);
            }
            log.error(e.getMessage(), e);
        }
        
        // pagesize validated in loadAndCreate
        return new AsyncResult<>(loadAndCreate(queryId, queryParameters));
    }
    
    /**
     * Returns a list of attribute names that can be used in subsequent queries
     *
     * @param id
     *            view, queryId, or alias
     * @return number of results, columns contained in the results
     *
     * @return {@code datawave.webservice.result.CachedResultsDescribeResponse<Description>}
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     *
     * @HTTP 200 success
     * @HTTP 500 internal server error
     *
     */
    @GET
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf"})
    @javax.ws.rs.Path("/{id}/describe")
    @GZIP
    @Interceptors({RequiredInterceptor.class, ResponseInterceptor.class})
    public CachedResultsDescribeResponse describe(@PathParam("id") @Required("id") String id) {
        // Find out who/what called this method
        Principal p = ctx.getCallerPrincipal();
        String owner = getOwnerFromPrincipal(p);
        
        CachedResultsDescribeResponse response = null;
        
        try {
            response = new CachedResultsDescribeResponse();
            CachedRunningQuery crq;
            try {
                crq = retrieve(id, owner);
            } catch (IOException e1) {
                throw new PreConditionFailedQueryException(DatawaveErrorCode.CACHED_RESULTS_IMPORT_ERROR, e1);
            }
            
            if (null == crq) {
                throw new NotFoundQueryException(DatawaveErrorCode.QUERY_OR_VIEW_NOT_FOUND);
            }
            
            if (!crq.getUser().equals(owner)) {
                throw new UnauthorizedQueryException(DatawaveErrorCode.QUERY_OWNER_MISMATCH, MessageFormat.format("{0} != {1}", crq.getUser(), owner));
            }
            
            String view = crq.getView();
            response.setView(view);
            
            List<String> columns = new ArrayList<>();
            Integer numRows = null;
            try (Connection con = ds.getConnection(); Statement s = con.createStatement()) {
                try (ResultSet rs = s.executeQuery("select count(*) from " + view)) {
                    if (rs.next()) {
                        numRows = rs.getInt(1);
                    }
                }
                
                try (ResultSet rs = s.executeQuery("show columns from " + view)) {
                    Set<String> fixedColumns = CacheableQueryRow.getFixedColumnSet();
                    while (rs.next()) {
                        String column = rs.getString(1);
                        if (!fixedColumns.contains(column)) {
                            columns.add(column);
                        }
                    }
                }
                
            } catch (SQLSyntaxErrorException e) {
                throw new NotFoundQueryException(DatawaveErrorCode.VIEW_NOT_FOUND);
            } catch (SQLException e) {
                throw new QueryException(DatawaveErrorCode.CACHED_QUERY_SQL_ERROR);
            }
            
            response.setColumns(columns);
            response.setNumRows(numRows);
        } catch (QueryException e) {
            QueryException qe = new QueryException(DatawaveErrorCode.CACHED_QUERY_DESCRIPTION_ERROR, e);
            response.addException(qe.getBottomQueryException());
            throw new DatawaveWebApplicationException(qe, response, e.getBottomQueryException().getStatusCode());
        }
        return response;
    }
    
    /**
     *
     * @param queryParameters
     *
     * @return datawave.webservice.result.CachedResultsResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @ResponseHeader query-session-id this header and value will be in the Set-Cookie header, subsequent calls for this session will need to supply the
     *                 query-session-id header in the request in a Cookie header or as a query parameter
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     *
     *                 'view' is a required parameter, however the caller may not know the view name. In this case, the caller may substitute the alias name
     *                 they created for the view. the retrieve call may retrieve using the alias, however other calls that operate on the actual view may not
     *                 substitute the alias (it is not the name of the table/view!) see comments inline below
     *
     * @HTTP 200 success
     * @HTTP 500 internal server error
     */
    @POST
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml"})
    @javax.ws.rs.Path("/{queryId}/create")
    @Interceptors(RequiredInterceptor.class)
    @GenerateQuerySessionId(cookieBasePath = "/DataWave/CachedResults/")
    @Timed(name = "dw.cachedr.create", absolute = true)
    public CachedResultsResponse create(@Required("queryId") @PathParam("queryId") String queryId, MultivaluedMap<String,String> queryParameters) {
        CreateQuerySessionIDFilter.QUERY_ID.set(null);
        
        queryParameters.putSingle(CachedResultsParameters.QUERY_ID, queryId);
        cp.clear();
        cp.validate(queryParameters);
        
        CachedResultsResponse response = new CachedResultsResponse();
        
        // Find out who/what called this method
        Principal p = ctx.getCallerPrincipal();
        String owner = getOwnerFromPrincipal(p);
        
        CachedRunningQuery crq = null;
        Connection con = null;
        try {
            con = ds.getConnection();
            CachedRunningQuery loadCrq = retrieve(cp.getView(), owner); // the caller may have used the alias name for the view.
            
            if (loadCrq == null) {
                throw new PreConditionFailedQueryException(DatawaveErrorCode.QUERY_NOT_CACHED);
            }
            if (!loadCrq.getUser().equals(owner)) {
                throw new UnauthorizedQueryException(DatawaveErrorCode.QUERY_OWNER_MISMATCH, MessageFormat.format("{0} != {1}", loadCrq.getUser(), owner));
            }
            
            if (cp.getPagesize() <= 0) {
                cp.setPagesize(cachedResultsConfiguration.getDefaultPageSize());
            }
            
            int maxPageSize = cachedResultsConfiguration.getMaxPageSize();
            if (maxPageSize > 0 && cp.getPagesize() > maxPageSize) {
                throw new PreConditionFailedQueryException(DatawaveErrorCode.REQUESTED_PAGE_SIZE_TOO_LARGE, MessageFormat.format("{0} > {1}.",
                                cp.getPagesize(), maxPageSize));
            }
            
            QueryLogic<?> queryLogic = loadCrq.getQueryLogic();
            String originalQueryId = loadCrq.getOriginalQueryId();
            Query query = loadCrq.getQuery();
            String table = loadCrq.getTableName();
            
            Set<String> fixedFields = null;
            if (!StringUtils.isEmpty(cp.getFixedFields())) {
                fixedFields = new HashSet<>();
                for (String field : cp.getFixedFields().split(",")) {
                    fixedFields.add(field.trim());
                }
            }
            // this needs the real view name, so use the value from loadCrq instead of cp.getView() (because cp.getView may return the alias instead)
            crq = new CachedRunningQuery(con, query, queryLogic, cp.getQueryId(), cp.getAlias(), owner, loadCrq.getView(), cp.getFields(), cp.getConditions(),
                            cp.getGrouping(), cp.getOrder(), cp.getPagesize(), loadCrq.getVariableFields(), fixedFields, metricFactory);
            crq.setStatus(CachedRunningQuery.Status.CREATING);
            crq.setOriginalQueryId(originalQueryId);
            crq.setTableName(table);
            persist(crq, owner);
            // see above comment about using loadCrq.getView() instead of cp.getView()
            CachedRunningQuery.removeFromDatabase(loadCrq.getView());
            
            crq.getMetric().setLifecycle(QueryMetric.Lifecycle.DEFINED);
            // see above comment about using loadCrq.getView() instead of cp.getView()
            String sqlQuery = crq.generateSql(loadCrq.getView(), cp.getFields(), cp.getConditions(), cp.getGrouping(), cp.getOrder(), owner, con);
            
            // Store the CachedRunningQuery in the cache under the user-supplied alias
            if (cp.getAlias() != null) {
                response.setAlias(cp.getAlias());
            }
            
            AuditType auditType = queryLogic.getAuditType(query);
            if (!auditType.equals(AuditType.NONE)) {
                // if auditType > AuditType.NONE, audit passively
                auditType = AuditType.PASSIVE;
                StringBuilder auditMessage = new StringBuilder();
                auditMessage.append("User running secondary query on cached results of original query,");
                auditMessage.append(" original query: ").append(query.getQuery());
                auditMessage.append(", secondary query: ").append(sqlQuery);
                MultivaluedMap<String,String> params = new MultivaluedMapImpl<>();
                params.putAll(query.toMap());
                marking.validate(params);
                PrivateAuditConstants.stripPrivateParameters(queryParameters);
                params.putSingle(PrivateAuditConstants.COLUMN_VISIBILITY, marking.toColumnVisibilityString());
                params.putSingle(PrivateAuditConstants.AUDIT_TYPE, auditType.name());
                params.putSingle(PrivateAuditConstants.USER_DN, query.getUserDN());
                params.putSingle(PrivateAuditConstants.LOGIC_CLASS, crq.getQueryLogic().getLogicName());
                params.remove(QueryParameters.QUERY_STRING);
                params.putSingle(QueryParameters.QUERY_STRING, auditMessage.toString());
                params.putAll(queryParameters);
                // if the user didn't set an audit id, use the query id
                if (!params.containsKey(AuditParameters.AUDIT_ID)) {
                    params.putSingle(AuditParameters.AUDIT_ID, queryId);
                }
                auditor.audit(params);
            }
            
            response.setOriginalQueryId(originalQueryId);
            response.setQueryId(cp.getQueryId());
            
            response.setViewName(loadCrq.getView());
            response.setTotalRows(crq.getTotalRows());
            
            crq.setStatus(CachedRunningQuery.Status.AVAILABLE);
            persist(crq, owner);
            CreateQuerySessionIDFilter.QUERY_ID.set(cp.getQueryId());
            return response;
            
        } catch (Exception e) {
            
            if (crq != null) {
                crq.getMetric().setError(e);
            }
            
            String statusMessage = e.getMessage();
            if (null == statusMessage) {
                statusMessage = e.getClass().getName();
            }
            
            try {
                persistByQueryId(cp.getQueryId(), cp.getAlias(), owner, CachedRunningQuery.Status.ERROR, statusMessage, true);
            } catch (IOException e1) {
                response.addException(new QueryException(DatawaveErrorCode.CACHED_QUERY_PERSISTANCE_ERROR, e1));
            }
            QueryException qe = new QueryException(DatawaveErrorCode.CACHED_QUERY_SET_ERROR, e);
            log.error(qe);
            response.addException(qe.getBottomQueryException());
            throw new DatawaveWebApplicationException(qe, response);
        } finally {
            crq.closeConnection(log);
            // Push metrics
            if (crq != null && crq.getQueryLogic().getCollectQueryMetrics() == true) {
                try {
                    metrics.updateMetric(crq.getMetric());
                } catch (Exception e1) {
                    log.error(e1.getMessage(), e1);
                }
            }
            
        }
        
    }
    
    /**
     * Update fields, conditions, grouping, or order for a CachedResults query. As a general rule, keep parens at least one space away from field names. Field
     * names also work with or without tick marks.
     *
     * @param queryId
     *            user defined id for this query
     * @param fields
     *            comma separated list of fields in the result set
     * @param conditions
     *            analogous to a SQL where clause
     * @param grouping
     *            comma separated list of fields to group by
     * @param order
     *            comma separated list of fields for ordering
     * @param pagesize
     *            size of returned pages
     *
     * @return datawave.webservice.result.CachedResultsResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @RequestHeader query-session-id session id value used for load balancing purposes. query-session-id can be placed in the request in a Cookie header or as
     *                a query parameter
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     * @ResponseHeader query-session-id this header and value will be in the Set-Cookie header, subsequent calls for this session will need to supply the
     *                 query-session-id header in the request in a Cookie header or as a query parameter
     *
     * @HTTP 200 success
     * @HTTP 401 caller is not authorized to run the query
     * @HTTP 412 if the query is not active
     * @HTTP 500 internal server error
     */
    @POST
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml"})
    @javax.ws.rs.Path("/{queryId}/update")
    @Interceptors({RequiredInterceptor.class, ResponseInterceptor.class})
    @GenerateQuerySessionId(cookieBasePath = "/DataWave/CachedResults/")
    @Timed(name = "dw.cachedr.update", absolute = true)
    public CachedResultsResponse update(@PathParam("queryId") @Required("queryId") String queryId, @FormParam("fields") String fields,
                    @FormParam("conditions") String conditions, @FormParam("grouping") String grouping, @FormParam("order") String order,
                    @FormParam("pagesize") Integer pagesize) {
        CreateQuerySessionIDFilter.QUERY_ID.set(null);
        
        boolean updated = false;
        
        CachedResultsResponse response = new CachedResultsResponse();
        
        // Find out who/what called this method
        Principal p = ctx.getCallerPrincipal();
        String owner = getOwnerFromPrincipal(p);
        
        try {
            
            CachedRunningQuery crq = null;
            try {
                
                crq = retrieve(queryId, owner);
                if (null == crq) {
                    throw new PreConditionFailedQueryException(DatawaveErrorCode.QUERY_NOT_CACHED);
                }
                if (!crq.getUser().equals(owner)) {
                    throw new UnauthorizedQueryException(DatawaveErrorCode.QUERY_OWNER_MISMATCH, MessageFormat.format("{0} != {1}", crq.getUser(), owner));
                }
                
                if (pagesize == null || pagesize <= 0) {
                    pagesize = cachedResultsConfiguration.getDefaultPageSize();
                }
                
                int maxPageSize = cachedResultsConfiguration.getMaxPageSize();
                if (maxPageSize > 0 && pagesize > maxPageSize) {
                    throw new PreConditionFailedQueryException(DatawaveErrorCode.REQUESTED_PAGE_SIZE_TOO_LARGE, MessageFormat.format("{0} > {1}.", pagesize,
                                    cachedResultsConfiguration.getMaxPageSize()));
                }
                
                synchronized (crq) {
                    
                    if (crq.isActivated() == false) {
                        Connection connection = ds.getConnection();
                        String logicName = crq.getQueryLogicName();
                        if (logicName != null) {
                            QueryLogic<?> queryLogic = queryFactory.getQueryLogic(logicName, p);
                            crq.activate(connection, queryLogic);
                        } else {
                            DbUtils.closeQuietly(connection);
                        }
                    }
                    
                    try {
                        if (fields == null && conditions == null && grouping == null && order == null) {
                            // don't do update
                        } else {
                            updated = crq.update(fields, conditions, grouping, order, pagesize);
                            persist(crq, owner);
                        }
                        
                        response.setOriginalQueryId(crq.getOriginalQueryId());
                        response.setQueryId(crq.getQueryId());
                        response.setViewName(crq.getView());
                        response.setAlias(crq.getAlias());
                        response.setTotalRows(crq.getTotalRows());
                    } finally {
                        // only close connection if the crq changed, because we expect additional actions
                        if (updated) {
                            crq.closeConnection(log);
                        }
                    }
                }
                
            } finally {
                if (crq != null && crq.getQueryLogic() != null && crq.getQueryLogic().getCollectQueryMetrics() == true) {
                    try {
                        metrics.updateMetric(crq.getMetric());
                    } catch (Exception e1) {
                        log.error("Error updating metrics", e1);
                    }
                }
            }
            CreateQuerySessionIDFilter.QUERY_ID.set(queryId);
            return response;
        } catch (Exception e) {
            QueryException qe = new QueryException(DatawaveErrorCode.CACHED_QUERY_UPDATE_ERROR, e);
            log.error(qe);
            response.addException(qe.getBottomQueryException());
            int statusCode = qe.getBottomQueryException().getStatusCode();
            throw new DatawaveWebApplicationException(qe, response, statusCode);
        }
    }
    
    /**
     *
     * Returns the previous page of results to the caller. The response object type is dynamic, see the listQueryLogic operation to determine what the response
     * type object will be.
     *
     * @param queryId
     *            user defined id for this query
     * @return previous page of results
     *
     * @return datawave.webservice.result.BaseQueryResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @RequestHeader query-session-id session id value used for load balancing purposes. query-session-id can be placed in the request in a Cookie header or as
     *                a query parameter
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     * @ResponseHeader X-query-page-number page number returned by this call
     * @ResponseHeader X-query-last-page if true then there are no more pages for this query, caller should call close()
     * @ResponseHeader X-Partial-Results true if the page contains less than the requested number of results
     *
     * @HTTP 200 success
     * @HTTP 401 caller is not authorized to run the query
     * @HTTP 412 if the query is not active
     * @HTTP 500 internal server error
     *
     */
    @GET
    @javax.ws.rs.Path("/{queryId}/previous")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf"})
    @GZIP
    @Interceptors({RequiredInterceptor.class, ResponseInterceptor.class})
    @Timed(name = "dw.cachedr.previous", absolute = true)
    public BaseQueryResponse previous(@PathParam("queryId") @Required("queryId") String queryId) {
        
        BaseQueryResponse response = responseObjectFactory.getEventQueryResponse();
        
        // Find out who/what called this method
        Principal p = ctx.getCallerPrincipal();
        String owner = getOwnerFromPrincipal(p);
        
        try {
            CachedRunningQuery crq = null;
            try {
                // Get the CachedRunningQuery object from the cache
                try {
                    crq = retrieve(queryId, owner);
                } catch (IOException e) {
                    throw new PreConditionFailedQueryException(DatawaveErrorCode.CACHED_RESULTS_IMPORT_ERROR, e);
                }
                if (null == crq)
                    throw new PreConditionFailedQueryException(DatawaveErrorCode.QUERY_NOT_CACHED);
                if (!crq.getUser().equals(owner)) {
                    throw new UnauthorizedQueryException(DatawaveErrorCode.QUERY_OWNER_MISMATCH, MessageFormat.format("{0} != {1}", crq.getUser(), owner));
                }
                
                synchronized (crq) {
                    if (crq.isActivated() == false) {
                        if (crq.getShouldAutoActivate()) {
                            Connection connection = ds.getConnection();
                            String logicName = crq.getQueryLogicName();
                            QueryLogic<?> queryLogic = queryFactory.getQueryLogic(logicName, p);
                            crq.activate(connection, queryLogic);
                        } else {
                            throw new PreConditionFailedQueryException(DatawaveErrorCode.QUERY_TIMEOUT_FOR_RESOURCES);
                        }
                    }
                    
                    try {
                        ResultsPage resultList = crq.previous(cachedResultsConfiguration.getPageByteTrigger());
                        long pageNum = crq.getLastPageNumber();
                        response = crq.getTransformer().createResponse(resultList);
                        Status status = null;
                        if (!resultList.getResults().isEmpty()) {
                            response.setHasResults(true);
                            status = Status.OK;
                        } else {
                            response.setHasResults(false);
                            status = Status.NO_CONTENT;
                        }
                        response.setPageNumber(pageNum);
                        response.setLogicName(crq.getQueryLogic().getLogicName());
                        response.setQueryId(crq.getQueryId());
                        if (response instanceof TotalResultsAware) {
                            ((TotalResultsAware) response).setTotalResults(crq.getTotalRows());
                        }
                        if (status == Status.NO_CONTENT) {
                            throw new NoResultsException(null);
                        }
                        crq.getMetric().setLifecycle(QueryMetric.Lifecycle.RESULTS);
                        return response;
                    } catch (SQLException e) {
                        throw new QueryException(DatawaveErrorCode.CACHED_QUERY_SQL_ERROR, e);
                    }
                }
            } finally {
                // Push metrics
                if (null != crq && crq.getQueryLogic().getCollectQueryMetrics() == true) {
                    
                    try {
                        metrics.updateMetric(crq.getMetric());
                    } catch (Exception e1) {
                        log.error("Error updating metrics", e1);
                    }
                }
            }
        } catch (Exception e) {
            QueryException qe = null;
            if (e instanceof NoResultsException) {
                qe = new QueryException(DatawaveErrorCode.NO_CONTENT_STATUS, e);
            } else {
                qe = new QueryException("Error calling previous.", e, "500-42");
                log.error(qe);
            }
            response.addException(qe.getBottomQueryException());
            int statusCode = qe.getBottomQueryException().getStatusCode();
            throw new DatawaveWebApplicationException(qe, response, statusCode);
        }
    }
    
    /**
     * Re-allocate resources for these cached results and reset paging to the beginning
     *
     * @param queryId
     *            user defined id for this query
     *
     * @return datawave.webservice.result.CachedResultsResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @ResponseHeader query-session-id this header and value will be in the Set-Cookie header, subsequent calls for this session will need to supply the
     *                 query-session-id header in the request in a Cookie header or as a query parameter
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     *
     * @HTTP 200 success
     * @HTTP 500 internal server error
     *
     */
    @PUT
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml"})
    @javax.ws.rs.Path("/{queryId}/reset")
    @Interceptors({RequiredInterceptor.class, ResponseInterceptor.class})
    @GenerateQuerySessionId(cookieBasePath = "/DataWave/CachedResults/")
    @Timed(name = "dw.cachedr.close", absolute = true)
    public CachedResultsResponse reset(@PathParam("queryId") @Required("queryId") String queryId) {
        CreateQuerySessionIDFilter.QUERY_ID.set(null);
        
        CachedResultsResponse response = new CachedResultsResponse();
        
        // Find out who/what called this method
        Principal p = ctx.getCallerPrincipal();
        String owner = getOwnerFromPrincipal(p);
        
        try {
            CachedRunningQuery crq = null;
            try {
                // Get the CachedRunningQuery object from the cache
                try {
                    crq = retrieve(queryId, owner);
                } catch (IOException e) {
                    throw new PreConditionFailedQueryException(DatawaveErrorCode.CACHED_RESULTS_IMPORT_ERROR);
                }
                if (null == crq)
                    throw new PreConditionFailedQueryException(DatawaveErrorCode.QUERY_NOT_CACHED);
                if (!crq.getUser().equals(owner)) {
                    throw new UnauthorizedQueryException(DatawaveErrorCode.QUERY_OWNER_MISMATCH, MessageFormat.format("{0} != {1}", crq.getUser(), owner));
                }
                
                synchronized (crq) {
                    
                    if (crq.isActivated() == true) {
                        crq.closeConnection(log);
                    }
                    
                    Connection connection = ds.getConnection();
                    String logicName = crq.getQueryLogicName();
                    QueryLogic<?> queryLogic = queryFactory.getQueryLogic(logicName, p);
                    crq.activate(connection, queryLogic);
                    
                    response.setQueryId(crq.getQueryId());
                    response.setOriginalQueryId(crq.getOriginalQueryId());
                    response.setViewName(crq.getView());
                    response.setAlias(crq.getAlias());
                    response.setTotalRows(crq.getTotalRows());
                    
                }
                
                CreateQuerySessionIDFilter.QUERY_ID.set(queryId);
                return response;
                
            } finally {
                // Push metrics
                if (null != crq && crq.getQueryLogic().getCollectQueryMetrics() == true) {
                    
                    try {
                        metrics.updateMetric(crq.getMetric());
                    } catch (Exception e1) {
                        log.error("Error updating metrics", e1);
                    }
                }
            }
        } catch (Exception e) {
            response.addMessage(e.getMessage());
            QueryException qe = new QueryException(DatawaveErrorCode.RESET_CALL_ERROR, e);
            log.error(qe);
            response.addException(qe.getBottomQueryException());
            int statusCode = qe.getBottomQueryException().getStatusCode();
            throw new DatawaveWebApplicationException(qe, response, statusCode);
        }
    }
    
    /**
     *
     * Returns the next page of results to the caller. The response object type is dynamic, see the listQueryLogic operation to determine what the response type
     * object will be.
     *
     * @param queryId
     *            user defined id for this query
     * @return a page of results
     *
     * @return datawave.webservice.result.BaseQueryResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @RequestHeader query-session-id session id value used for load balancing purposes. query-session-id can be placed in the request in a Cookie header or as
     *                a query parameter
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     * @ResponseHeader X-query-page-number page number returned by this call
     * @ResponseHeader X-query-last-page if true then there are no more pages for this query, caller should call close()
     * @ResponseHeader X-Partial-Results true if the page contains less than the requested number of results
     *
     * @HTTP 200 success
     * @HTTP 401 caller is not authorized to run the query
     * @HTTP 412 if the query is not active
     * @HTTP 500 internal server error
     *
     */
    @GET
    @javax.ws.rs.Path("/{queryId}/next")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf"})
    @GZIP
    @Interceptors({RequiredInterceptor.class, ResponseInterceptor.class})
    @Timed(name = "dw.cachedr.next", absolute = true)
    public BaseQueryResponse next(@PathParam("queryId") @Required("queryId") String queryId) {
        
        BaseQueryResponse response = responseObjectFactory.getEventQueryResponse();
        
        // Find out who/what called this method
        Principal p = ctx.getCallerPrincipal();
        String owner = getOwnerFromPrincipal(p);
        
        try {
            CachedRunningQuery crq = null;
            try {
                // Get the CachedRunningQuery object from the cache
                try {
                    crq = retrieve(queryId, owner);
                } catch (IOException e) {
                    throw new PreConditionFailedQueryException(DatawaveErrorCode.CACHED_RESULTS_IMPORT_ERROR, e);
                }
                if (null == crq)
                    throw new PreConditionFailedQueryException(DatawaveErrorCode.QUERY_NOT_CACHED);
                
                response = crq.getQueryLogic().getResponseObjectFactory().getEventQueryResponse();
                
                if (!crq.getUser().equals(owner)) {
                    throw new UnauthorizedQueryException(DatawaveErrorCode.QUERY_OWNER_MISMATCH, MessageFormat.format("{0} != {1}", crq.getUser(), owner));
                }
                
                synchronized (crq) {
                    if (crq.isActivated() == false) {
                        if (crq.getShouldAutoActivate()) {
                            Connection connection = ds.getConnection();
                            String logicName = crq.getQueryLogicName();
                            QueryLogic<?> queryLogic = queryFactory.getQueryLogic(logicName, p);
                            crq.activate(connection, queryLogic);
                        } else {
                            throw new PreConditionFailedQueryException(DatawaveErrorCode.QUERY_TIMEOUT_FOR_RESOURCES);
                        }
                    }
                    
                    try {
                        ResultsPage resultList = crq.next(cachedResultsConfiguration.getPageByteTrigger());
                        long pageNum = crq.getLastPageNumber();
                        response = crq.getTransformer().createResponse(resultList);
                        Status status = null;
                        if (!resultList.getResults().isEmpty()) {
                            response.setHasResults(true);
                            status = Status.OK;
                        } else {
                            response.setHasResults(false);
                            status = Status.NO_CONTENT;
                        }
                        response.setPageNumber(pageNum);
                        response.setLogicName(crq.getQueryLogic().getLogicName());
                        response.setQueryId(crq.getQueryId());
                        if (response instanceof TotalResultsAware) {
                            ((TotalResultsAware) response).setTotalResults(crq.getTotalRows());
                        }
                        if (status == Status.NO_CONTENT) {
                            throw new NoResultsException(null);
                        }
                        crq.getMetric().setLifecycle(QueryMetric.Lifecycle.RESULTS);
                        return response;
                    } catch (SQLException e) {
                        throw new QueryException(DatawaveErrorCode.NEXT_CALL_ERROR, e);
                    }
                }
            } finally {
                
                // Push metrics
                if (null != crq && crq.getQueryLogic().getCollectQueryMetrics() == true) {
                    try {
                        metrics.updateMetric(crq.getMetric());
                    } catch (Exception e1) {
                        log.error("Error updating metrics", e1);
                    }
                }
            }
        } catch (Exception e) {
            QueryException qe = null;
            if (e instanceof NoResultsException) {
                qe = new QueryException(DatawaveErrorCode.NO_CONTENT_STATUS, e);
            } else {
                qe = new QueryException(DatawaveErrorCode.NEXT_CALL_ERROR, e);
                log.error(qe);
            }
            response.addException(qe.getBottomQueryException());
            int statusCode = qe.getBottomQueryException().getStatusCode();
            throw new DatawaveWebApplicationException(qe, response, statusCode);
        }
    }
    
    /**
     * Returns a set of rows based on the given starting and end positions. The response object type is dynamic, see the listQueryLogic operation to determine
     * what the response type object will be.
     *
     * @param queryId
     *            CachedResults queryId
     * @param rowBegin
     *            first row to be returned
     * @param rowEnd
     *            last row to be returned
     * @return datawave.webservice.result.BaseQueryResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @RequestHeader query-session-id session id value used for load balancing purposes. query-session-id can be placed in the request in a Cookie header or as
     *                a query parameter
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     * @ResponseHeader X-Partial-Results true if the page contains less than the requested number of results
     *
     * @HTTP 200 success
     * @HTTP 401 caller is not authorized to run the query
     * @HTTP 412 if the query is not active
     * @HTTP 500 internal server error
     */
    @GET
    @javax.ws.rs.Path("/{queryId}/getRows")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf"})
    @GZIP
    @Interceptors({RequiredInterceptor.class, ResponseInterceptor.class})
    @Timed(name = "dw.cachedr.getRows", absolute = true)
    public BaseQueryResponse getRows(@PathParam("queryId") @Required("queryId") String queryId, @QueryParam("rowBegin") @DefaultValue("1") Integer rowBegin,
                    @QueryParam("rowEnd") Integer rowEnd) {
        
        BaseQueryResponse response = responseObjectFactory.getEventQueryResponse();
        
        // Find out who/what called this method
        Principal p = ctx.getCallerPrincipal();
        String owner = getOwnerFromPrincipal(p);
        
        try {
            if (rowBegin < 1) {
                throw new BadRequestQueryException(DatawaveErrorCode.ROW_BEGIN_LESS_THAN_1);
            }
            if (rowEnd != null && rowEnd < rowBegin) {
                throw new BadRequestQueryException(DatawaveErrorCode.ROW_END_LESS_THAN_ROW_BEGIN);
            }
            
            // If there is a this.maxPageSize set, then we should honor it here. Otherwise, we use Integer.MAX_VALUE
            int maxPageSize = cachedResultsConfiguration.getMaxPageSize();
            if (rowEnd == null) {
                if (maxPageSize > 0) {
                    rowEnd = (rowBegin + maxPageSize) - 1;
                } else {
                    rowEnd = Integer.MAX_VALUE;
                }
            }
            int pagesize = (rowEnd - rowBegin) + 1;
            if (maxPageSize > 0 && pagesize > maxPageSize) {
                throw new QueryException(DatawaveErrorCode.TOO_MANY_ROWS_REQUESTED,
                                MessageFormat.format("Size must be less than or equal to: {0}", maxPageSize));
            }
            
            CachedRunningQuery crq = null;
            try {
                // Get the CachedRunningQuery object from the cache
                try {
                    crq = retrieve(queryId, owner);
                } catch (IOException e) {
                    throw new PreConditionFailedQueryException(DatawaveErrorCode.CACHED_RESULTS_IMPORT_ERROR, e);
                }
                
                if (null == crq)
                    throw new PreConditionFailedQueryException(DatawaveErrorCode.QUERY_NOT_CACHED);
                
                response = crq.getQueryLogic().getResponseObjectFactory().getEventQueryResponse();
                
                if (!crq.getUser().equals(owner)) {
                    throw new UnauthorizedQueryException(DatawaveErrorCode.QUERY_OWNER_MISMATCH, MessageFormat.format("{0} != {1}", crq.getUser(), owner));
                }
                
                synchronized (crq) {
                    
                    if (crq.isActivated() == false) {
                        Connection connection = ds.getConnection();
                        String logicName = crq.getQueryLogicName();
                        QueryLogic<?> queryLogic = queryFactory.getQueryLogic(logicName, p);
                        crq.activate(connection, queryLogic);
                    }
                    
                    try {
                        ResultsPage resultList = crq.getRows(rowBegin, rowEnd, cachedResultsConfiguration.getPageByteTrigger());
                        response = crq.getTransformer().createResponse(resultList);
                        Status status;
                        if (!resultList.getResults().isEmpty()) {
                            response.setHasResults(true);
                            status = Status.OK;
                        } else {
                            response.setHasResults(false);
                            status = Status.NO_CONTENT;
                        }
                        response.setLogicName(crq.getQueryLogic().getLogicName());
                        response.setQueryId(crq.getQueryId());
                        if (response instanceof TotalResultsAware) {
                            ((TotalResultsAware) response).setTotalResults(crq.getTotalRows());
                        }
                        if (status == Status.NO_CONTENT) {
                            throw new NoResultsQueryException(DatawaveErrorCode.NO_CONTENT_STATUS);
                        }
                        crq.getMetric().setLifecycle(QueryMetric.Lifecycle.RESULTS);
                        return response;
                        
                    } catch (SQLException e) {
                        throw new QueryException();
                    } catch (NoResultsQueryException e) {
                        throw e;
                    } catch (RuntimeException e) {
                        log.error(e.getMessage(), e);
                        throw e;
                    } finally {
                        crq.closeConnection(log);
                    }
                }
            } finally {
                // Push metrics
                try {
                    if (null != crq && crq.getQueryLogic().getCollectQueryMetrics() == true) {
                        metrics.updateMetric(crq.getMetric());
                    }
                } catch (Exception e1) {
                    log.error("Error updating metrics", e1);
                }
            }
        } catch (Exception e) {
            DatawaveErrorCode dec;
            if (e instanceof NoResultsQueryException) {
                dec = DatawaveErrorCode.NO_CONTENT_STATUS;
            } else {
                dec = DatawaveErrorCode.CACHED_QUERY_TRANSACTION_ERROR;
            }
            QueryException qe = new QueryException(dec, e);
            log.error(qe);
            response.addException(qe.getBottomQueryException());
            int statusCode = qe.getBottomQueryException().getStatusCode();
            throw new DatawaveWebApplicationException(qe, response, statusCode);
        }
    }
    
    /**
     * Cancel the load process.
     *
     * @param originalQueryId
     *
     * @return datawave.webservice.result.VoidResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @RequestHeader query-session-id session id value used for load balancing purposes. query-session-id can be placed in the request in a Cookie header or as
     *                a query parameter
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     *
     * @HTTP 200 success
     * @HTTP 401 caller is not authorized to cancel the query
     */
    @PUT
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @javax.ws.rs.Path("/{queryId}/cancel")
    @GZIP
    @ClearQuerySessionId
    @Interceptors({RequiredInterceptor.class, ResponseInterceptor.class})
    @Timed(name = "dw.cachedr.cancel", absolute = true)
    public VoidResponse cancelLoad(@PathParam("queryId") @Required("queryId") String originalQueryId) {
        // Find out who/what called this method
        Principal p = ctx.getCallerPrincipal();
        String owner = getOwnerFromPrincipal(p);
        
        VoidResponse response = new VoidResponse();
        
        try {
            // check if query is even loading
            RunningQuery query = CachedResultsBean.loadingQueryMap.get(originalQueryId);
            
            if (query == null) {
                NotFoundQueryException e = new NotFoundQueryException(DatawaveErrorCode.NO_QUERY_OBJECT_MATCH);
                throw new NotFoundException(e, response);
            } else {
                if (query.getSettings().getOwner().equals(owner)) {
                    accumuloConnectionRequestBean.cancelConnectionRequest(originalQueryId);
                    query.cancel();
                    response.addMessage("CachedResults load canceled.");
                } else {
                    UnauthorizedQueryException e = new UnauthorizedQueryException(DatawaveErrorCode.QUERY_OWNER_MISMATCH, MessageFormat.format("{0} != {1}",
                                    query.getSettings().getOwner(), owner));
                    throw new UnauthorizedException(e, response);
                }
            }
            return response;
        } catch (DatawaveWebApplicationException e) {
            throw e;
        } catch (Exception e) {
            QueryException qe = new QueryException(DatawaveErrorCode.CANCELLATION_ERROR, e, MessageFormat.format("query_id: {0}", originalQueryId));
            log.error(qe);
            response.addException(qe.getBottomQueryException());
            int statusCode = qe.getBottomQueryException().getStatusCode();
            throw new DatawaveWebApplicationException(qe, response, statusCode);
        }
    }
    
    /**
     * <strong>JBossAdministrator or Administrator credentials required.</strong> Cancel the load process
     *
     * @param originalQueryId
     *
     * @return datawave.webservice.result.VoidResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for a user by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @RequestHeader query-session-id session id value used for load balancing purposes. query-session-id can be placed in the request in a Cookie header or as
     *                a query parameter
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     *
     * @HTTP 200 success
     */
    @PUT
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @javax.ws.rs.Path("/Admin/{queryId}/cancel")
    @GZIP
    @RolesAllowed({"InternalUser", "Administrator", "JBossAdministrator"})
    @Interceptors({RequiredInterceptor.class, ResponseInterceptor.class})
    public VoidResponse cancelLoadByAdmin(@PathParam("queryId") @Required("queryId") String originalQueryId) {
        
        VoidResponse response = new VoidResponse();
        
        try {
            // check if query is even loading
            RunningQuery query = CachedResultsBean.loadingQueryMap.get(originalQueryId);
            
            if (query == null) {
                NotFoundQueryException e = new NotFoundQueryException(DatawaveErrorCode.NO_QUERY_OBJECT_MATCH);
                throw new NotFoundException(e, response);
            } else {
                accumuloConnectionRequestBean.adminCancelConnectionRequest(originalQueryId);
                query.cancel();
                response.addMessage("CachedResults load canceled.");
            }
            return response;
        } catch (DatawaveWebApplicationException e) {
            throw e;
        } catch (Exception e) {
            QueryException qe = new QueryException(DatawaveErrorCode.CANCELLATION_ERROR, e, MessageFormat.format("query_id: {0}", originalQueryId));
            log.error(qe);
            response.addException(qe.getBottomQueryException());
            int statusCode = qe.getBottomQueryException().getStatusCode();
            throw new DatawaveWebApplicationException(qe, response, statusCode);
        }
    }
    
    @RolesAllowed({"InternalUser"})
    public boolean isQueryLoading(String originalQueryId) {
        return CachedResultsBean.loadingQueryMap.containsKey(originalQueryId);
    }
    
    /**
     * Releases resources associated with this query.
     *
     * @param queryId
     *            use defined id for this query
     *
     * @return datawave.webservice.result.VoidResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @RequestHeader query-session-id session id value used for load balancing purposes. query-session-id can be placed in the request in a Cookie header or as
     *                a query parameter
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     *
     * @HTTP 200 success
     * @HTTP 500 internal server error
     *
     */
    @DELETE
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml"})
    @javax.ws.rs.Path("/{queryId}/close")
    @Interceptors({RequiredInterceptor.class, ResponseInterceptor.class})
    @ClearQuerySessionId
    @Timed(name = "dw.cachedr.close", absolute = true)
    public VoidResponse close(@PathParam("queryId") @Required("queryId") String queryId) {
        // Find out who/what called this method
        Principal p = ctx.getCallerPrincipal();
        String owner = getOwnerFromPrincipal(p);
        VoidResponse response = new VoidResponse();
        CachedRunningQuery crq;
        try {
            crq = retrieve(queryId, owner);
        } catch (IOException e) {
            PreConditionFailedQueryException qe = new PreConditionFailedQueryException(DatawaveErrorCode.CACHED_RUNNING_QUERY_ERROR, e);
            log.error(qe);
            response.addException(qe.getBottomQueryException());
            throw new PreConditionFailedException(qe, response);
        }
        
        if (null != crq) {
            // CachedRunningQueries may be stored under multiple keys
            if (crq.getQueryId() != null) {
                cachedRunningQueryCache.remove(owner + "-" + crq.getQueryId());
            }
            if (crq.getAlias() != null) {
                cachedRunningQueryCache.remove(owner + "-" + crq.getAlias());
            }
            if (crq.getView() != null) {
                cachedRunningQueryCache.remove(owner + "-" + crq.getView());
            }
            if (crq.isActivated()) {
                synchronized (crq) {
                    crq.closeConnection(log);
                }
                crq.getMetric().setLifecycle(QueryMetric.Lifecycle.CLOSED);
                if (crq.getQueryLogic().getCollectQueryMetrics() == true) {
                    try {
                        metrics.updateMetric(crq.getMetric());
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                }
            }
        }
        return response;
    }
    
    private RunningQuery getQueryById(String id) throws Exception {
        // Find out who/what called this method
        Principal p = ctx.getCallerPrincipal();
        String owner = getOwnerFromPrincipal(p);
        Collection<Collection<String>> cbAuths = new HashSet<>();
        if (p instanceof DatawavePrincipal) {
            DatawavePrincipal cp = (DatawavePrincipal) p;
            cbAuths.addAll(cp.getAuthorizations());
        }
        
        if (log.isTraceEnabled()) {
            log.trace(owner + " has authorizations " + cbAuths);
        }
        
        RunningQuery query = runningQueryCache.get(id);
        
        if (null == query) {
            List<Query> queries = persister.findById(id);
            if (null == queries || queries.isEmpty())
                throw new NotFoundQueryException(DatawaveErrorCode.NO_QUERY_OBJECT_MATCH);
            if (queries.size() > 1)
                throw new NotFoundQueryException(DatawaveErrorCode.TOO_MANY_QUERY_OBJECT_MATCHES);
            else {
                Query q = queries.get(0);
                
                // will throw IllegalArgumentException if not defined
                QueryLogic<?> logic = queryFactory.getQueryLogic(q.getQueryLogicName(), p);
                AccumuloConnectionFactory.Priority priority = logic.getConnectionPriority();
                query = new RunningQuery(metrics, null, priority, logic, q, q.getQueryAuthorizations(), p, new RunningQueryTimingImpl(queryExpirationConf,
                                q.getPageTimeout()), executor, predictor, metricFactory);
                query.setActiveCall(true);
                // Put in the cache by id and name, we will have two copies that reference the same object
                runningQueryCache.put(q.getId().toString(), query);
            }
        } else {
            // Check to make sure that this query belongs to current user.
            if (!query.getSettings().getOwner().equals(owner)) {
                throw new UnauthorizedQueryException(DatawaveErrorCode.QUERY_OWNER_MISMATCH, MessageFormat.format("{0} != {1}", owner, query.getSettings()
                                .getOwner()));
            }
        }
        query.setActiveCall(false);
        return query;
    }
    
    /**
     * Set alias that this cached result can be retrieved by
     *
     * @param queryId
     *            user defined id for this query
     * @param alias
     *            additional name that this query can be retrieved by
     * @return datawave.webservice.result.CachedResultsResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @RequestHeader query-session-id session id value used for load balancing purposes. query-session-id can be placed in the request in a Cookie header or as
     *                a query parameter
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     *
     * @HTTP 200 success
     * @HTTP 401 caller is not authorized to run the query
     * @HTTP 412 if the query is not active
     * @HTTP 500 internal server error
     */
    @POST
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml"})
    @javax.ws.rs.Path("/{queryId}/setAlias")
    @Interceptors({RequiredInterceptor.class, ResponseInterceptor.class})
    @Timed(name = "dw.cachedr.setAlias", absolute = true)
    public CachedResultsResponse setAlias(@PathParam("queryId") @Required("queryId") String queryId, @FormParam("alias") @Required("alias") String alias) {
        
        CachedResultsResponse response = new CachedResultsResponse();
        
        // Find out who/what called this method
        Principal p = ctx.getCallerPrincipal();
        String owner = getOwnerFromPrincipal(p);
        
        try {
            CachedRunningQuery crq = null;
            try {
                // Get the CachedRunningQuery object from the cache
                try {
                    crq = retrieve(queryId, owner);
                } catch (IOException e) {
                    throw new PreConditionFailedQueryException(DatawaveErrorCode.CACHED_RESULTS_IMPORT_ERROR, e);
                }
                if (null == crq)
                    throw new PreConditionFailedQueryException(DatawaveErrorCode.QUERY_NOT_CACHED);
                if (!crq.getUser().equals(owner)) {
                    throw new UnauthorizedQueryException(DatawaveErrorCode.QUERY_OWNER_MISMATCH, MessageFormat.format("{0} != {1}", crq.getUser(), owner));
                }
                
                synchronized (crq) {
                    
                    if (alias != null) {
                        
                        crq.setAlias(alias);
                        persist(crq, owner);
                    }
                    
                    response = new CachedResultsResponse();
                    response.setOriginalQueryId(crq.getOriginalQueryId());
                    response.setQueryId(crq.getQueryId());
                    response.setViewName(crq.getView());
                    response.setAlias(crq.getAlias());
                    response.setTotalRows(crq.getTotalRows());
                    
                }
                
                return response;
            } finally {
                // Push metrics
                if (null != crq && crq.getQueryLogic().getCollectQueryMetrics() == true) {
                    
                    try {
                        metrics.updateMetric(crq.getMetric());
                    } catch (Exception e1) {
                        log.error("Error updating metrics", e1);
                    }
                }
            }
        } catch (Exception e) {
            QueryException qe = new QueryException(DatawaveErrorCode.CACHED_QUERY_TRANSACTION_ERROR, e);
            log.error(qe);
            response.addException(qe.getBottomQueryException());
            int statusCode = qe.getBottomQueryException().getStatusCode();
            throw new DatawaveWebApplicationException(qe, response, statusCode);
        }
    }
    
    public void persist(CachedRunningQuery crq, String owner) {
        
        synchronized (this) {
            log.debug("persisting cachedRunningQuery " + crq.getQueryId() + " to cache with status " + crq.getStatus());
            this.cachedRunningQueryCache.remove(owner + "-" + crq.getQueryId());
            this.cachedRunningQueryCache.remove(owner + "-" + crq.getAlias());
            this.cachedRunningQueryCache.remove(owner + "-" + crq.getView());
            
            this.cachedRunningQueryCache.put(owner + "-" + crq.getQueryId(), crq);
            this.cachedRunningQueryCache.put(owner + "-" + crq.getAlias(), crq);
            this.cachedRunningQueryCache.put(owner + "-" + crq.getView(), crq);
            log.debug("persisting cachedRunningQuery " + crq.getQueryId() + " to database with status " + crq.getStatus());
            crq.saveToDatabase(ctx.getCallerPrincipal(), metricFactory);
        }
        
    }
    
    public CachedRunningQuery retrieve(String id, String owner) throws IOException {
        
        CachedRunningQuery crq;
        
        synchronized (CachedResultsBean.class) {
            try {
                log.debug("retrieving cachedRunningQuery " + id + " from cache");
                crq = this.cachedRunningQueryCache.get(owner + "-" + id);
                if (crq != null) {
                    log.debug("retrieved cachedRunningQuery " + id + " from cache with status " + crq.getStatus());
                }
            } catch (Exception e) {
                log.error("Caught attempting to retrieve cached results from infinispan cache: " + e.getMessage(), e);
                throw new IOException(e.getClass().getName() + " caught attempting to retrieve cached results from infinispan cache", e);
            }
            
            log.debug("Details not in cache, checking the database");
            if (crq == null) {
                
                try {
                    log.debug("retrieving cachedRunningQuery " + id + " from database");
                    crq = CachedRunningQuery.retrieveFromDatabase(id, ctx.getCallerPrincipal(), metricFactory);
                    if (crq != null) {
                        log.debug("retrieved cachedRunningQuery " + id + " from database with status " + crq.getStatus());
                        this.cachedRunningQueryCache.put(owner + "-" + id, crq);
                    }
                } catch (Exception e) {
                    throw new IOException(e.getClass().getName() + " caught attempting to retrieve cached results state from database", e);
                }
            }
        }
        
        if (null == crq) {
            try {
                log.debug("Details not in database, checking for exported table in HDFS");
                String hdfsUri = cachedResultsConfiguration.getParameters().get("HDFS_URI");
                String hdfsDir = cachedResultsConfiguration.getParameters().get("HDFS_DIR");
                if (log.isDebugEnabled())
                    log.debug("HDFS uri: " + hdfsUri + ", dir: " + hdfsDir);
                if (!StringUtils.isEmpty(hdfsUri) && !StringUtils.isEmpty(hdfsDir) && (null != importFileUrl)) {
                    // Then we didn't find it in the database. Try looking in HDFS for an export of the data
                    // from another instance.
                    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
                    conf.set("fs.defaultFS", hdfsUri);
                    final FileSystem fs = FileSystem.get(conf);
                    Path crPath = new Path(hdfsDir);
                    Path userPath = new Path(crPath, owner);
                    if (log.isDebugEnabled())
                        log.debug("Looking for exported cached for " + id + " results in: " + userPath);
                    if (fs.exists(userPath)) {
                        // Find any directory that contains the id as a .alias, .view, or .queryId file
                        ArrayList<FileStatus> list = new ArrayList<>();
                        recursiveList(fs, userPath, id, list);
                        if (null != list && !list.isEmpty()) {
                            for (FileStatus f : list) {
                                // Get the parent of the matching file, this will be the directory that contains
                                // the table dump
                                Path tableDirectory = f.getPath().getParent();
                                ProcessBuilder builder = new ProcessBuilder(this.importFileUrl.getPath(), tableDirectory.toString());
                                builder.redirectErrorStream(false);
                                Process process = builder.start();
                                InputStream stdout = process.getInputStream();
                                final BufferedReader outReader = new BufferedReader(new InputStreamReader(stdout));
                                InputStream stderr = process.getErrorStream();
                                final BufferedReader errReader = new BufferedReader(new InputStreamReader(stderr));
                                try {
                                    Thread outReadThread = new Thread(() -> {
                                        try {
                                            String line = outReader.readLine();
                                            while (line != null) {
                                                log.info(line);
                                                line = outReader.readLine();
                                            }
                                        } catch (IOException e) {
                                            log.error("Error in readThread", e);
                                        }
                                    });
                                    Thread errReadThread = new Thread(() -> {
                                        try {
                                            String line = errReader.readLine();
                                            while (line != null) {
                                                log.error(line);
                                                line = errReader.readLine();
                                            }
                                        } catch (IOException e) {
                                            log.error("Error in readThread", e);
                                        }
                                    });
                                    outReadThread.setName(id + "-StdOutReadThread");
                                    outReadThread.start();
                                    errReadThread.setName(id + "-StdErrReadThread");
                                    errReadThread.start();
                                    log.info("Importing cached results from: " + tableDirectory);
                                    int exitVal = process.waitFor();
                                    if (0 != exitVal) {
                                        throw new IOException("Error importing cached results data from: " + tableDirectory + ". Exit value: " + exitVal);
                                    }
                                    outReadThread.join();
                                    errReadThread.join();
                                } catch (InterruptedException e) {
                                    log.error("Thread interrupted waiting for import to finish, killing import process");
                                    process.destroy();
                                } finally {
                                    outReader.close();
                                    errReader.close();
                                }
                                crq = CachedRunningQuery.retrieveFromDatabase(id, ctx.getCallerPrincipal(), metricFactory);
                                if (crq != null) {
                                    break;
                                }
                            }
                        } else {
                            // No matching table dumps for this user
                        }
                    } else {
                        // Does not exist
                    }
                } else {
                    log.warn("HDFS Parameters not set up, will not try to import data");
                }
                if (crq != null) {
                    synchronized (CachedResultsBean.class) {
                        this.cachedRunningQueryCache.put(owner + "-" + id, crq);
                    }
                }
            } catch (Exception e) {
                throw new IOException(e.getClass().getName() + " caught attempting to retrieve cached results state from hdfs", e);
            }
        }
        return crq;
    }
    
    private void recursiveList(FileSystem fs, Path p, String id, ArrayList<FileStatus> results) throws IOException {
        if (log.isDebugEnabled())
            log.debug("Checking path: " + p.getName());
        FileStatus[] list = fs.listStatus(p);
        if (null != list && list.length > 0) {
            for (FileStatus stat : list) {
                if (stat.isDirectory()) {
                    log.debug(stat.getPath().getName() + " is a directory");
                    recursiveList(fs, stat.getPath(), id, results);
                } else {
                    log.debug(stat.getPath().getName() + " is not a directory");
                    if (stat.getPath().getName().equals(id + ".alias") || stat.getPath().getName().equals(id + ".view")
                                    || stat.getPath().getName().equals(id + ".queryId")) {
                        results.add(stat);
                    } else {
                        log.debug(stat.getPath().getName() + " does not match filter");
                    }
                }
            }
        }
    }
    
    public void persistByQueryId(String queryId, String alias, String owner, CachedRunningQuery.Status status, String statusMessage, boolean useCache)
                    throws IOException {
        
        CachedRunningQuery crq = null;
        
        synchronized (CachedResultsBean.class) {
            log.debug("persisting cachedRunningQuery " + queryId + " to cache with status " + status);
            if (useCache) {
                crq = retrieve(queryId, owner);
            }
            
            if (crq != null) {
                crq.setStatus(status);
                crq.setStatusMessage(statusMessage);
                
                this.cachedRunningQueryCache.remove(owner + "-" + crq.getQueryId());
                this.cachedRunningQueryCache.remove(owner + "-" + crq.getAlias());
                this.cachedRunningQueryCache.remove(owner + "-" + crq.getView());
                
                this.cachedRunningQueryCache.put(owner + "-" + crq.getQueryId(), crq);
                this.cachedRunningQueryCache.put(owner + "-" + crq.getAlias(), crq);
                this.cachedRunningQueryCache.put(owner + "-" + crq.getView(), crq);
            }
            
            log.debug("persisting cachedRunningQuery " + queryId + " to database with status " + status);
            CachedRunningQuery.saveToDatabaseByQueryId(queryId, alias, owner, status, statusMessage);
        }
    }
    
    protected boolean createView(String tableName, String viewName, Connection con, boolean viewCreated, Map<String,Integer> fieldMap) throws SQLException {
        CachedResultsParameters.validate(tableName);
        CachedResultsParameters.validate(viewName);
        StringBuilder viewCols = new StringBuilder();
        StringBuilder tableCols = new StringBuilder();
        viewCols.append(BASE_COLUMNS);
        tableCols.append(BASE_COLUMNS);
        String sep = COMMA;
        for (Entry<String,Integer> e : fieldMap.entrySet()) {
            viewCols.append(sep).append("`").append(e.getKey()).append("`");
            tableCols.append(sep).append(FIELD).append(e.getValue() - CacheableQueryRow.getFixedColumnSet().size() - 1);
        }
        
        StringBuilder view = new StringBuilder();
        try {
            view.append("CREATE VIEW ").append(viewName).append("(");
            view.append(viewCols);
            view.append(") AS SELECT ").append(tableCols);
            view.append(" FROM ").append(tableName);
            if (log.isTraceEnabled()) {
                log.trace("Creating view using sql: " + view);
            }
            try (Statement viewStmt = con.createStatement()) {
                viewStmt.execute(view.toString());
            }
            viewCreated = true;
        } catch (SQLException e) {
            log.error("Error creating view with sql: " + view, e);
            throw e;
        }
        return viewCreated;
    }
    
    private void addQueryToTrackingMap(Map<String,String> trackingMap, Query q) {
        
        if (trackingMap == null || q == null) {
            return;
        }
        
        if (q.getOwner() != null) {
            trackingMap.put("query.user", q.getOwner());
        }
        if (q.getId() != null) {
            trackingMap.put("query.id", q.getId().toString());
        }
        if (q.getId() != null) {
            trackingMap.put("query.query", q.getQuery());
        }
    }
    
    public QueryPredictor getPredictor() {
        return predictor;
    }
    
    public void setPredictor(QueryPredictor predictor) {
        this.predictor = predictor;
    }
    
}
