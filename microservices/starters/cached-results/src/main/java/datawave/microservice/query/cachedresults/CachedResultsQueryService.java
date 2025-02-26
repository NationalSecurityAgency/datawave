package datawave.microservice.query.cachedresults;

import static datawave.microservice.query.QueryParameters.QUERY_VISIBILITY;
import static datawave.microservice.query.cachedresults.status.CachedResultsQueryStatus.CACHED_RESULTS_STATE.CANCELED;
import static datawave.microservice.query.cachedresults.status.CachedResultsQueryStatus.CACHED_RESULTS_STATE.CREATED;
import static datawave.microservice.query.cachedresults.status.CachedResultsQueryStatus.CACHED_RESULTS_STATE.CREATING;
import static datawave.microservice.query.cachedresults.status.CachedResultsQueryStatus.CACHED_RESULTS_STATE.FAILED;
import static datawave.microservice.query.cachedresults.status.CachedResultsQueryStatus.CACHED_RESULTS_STATE.LOADED;
import static datawave.microservice.query.cachedresults.status.CachedResultsQueryStatus.CACHED_RESULTS_STATE.LOADING;
import static datawave.microservice.query.cachedresults.status.CachedResultsQueryStatus.CACHED_RESULTS_STATE.NONE;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.commons.collections4.Transformer;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import datawave.core.common.audit.PrivateAuditConstants;
import datawave.core.query.cache.ResultsPage;
import datawave.core.query.cachedresults.CacheableLogic;
import datawave.core.query.cachedresults.CacheableQueryRowReader;
import datawave.core.query.cachedresults.CachedResultsQueryParameters;
import datawave.core.query.logic.QueryLogic;
import datawave.core.query.logic.QueryLogicFactory;
import datawave.marking.MarkingFunctions;
import datawave.marking.SecurityMarking;
import datawave.microservice.audit.AuditClient;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.config.RequestScopeBeanSupplier;
import datawave.microservice.query.cachedresults.config.CachedResultsQueryProperties;
import datawave.microservice.query.cachedresults.status.CachedResultsQueryStatus;
import datawave.microservice.query.cachedresults.status.cache.CachedResultsQueryCache;
import datawave.microservice.query.cachedresults.status.cache.util.CacheUpdater;
import datawave.microservice.query.storage.QueryStatus;
import datawave.microservice.query.storage.QueryStorageCache;
import datawave.security.authorization.ProxiedUserDetails;
import datawave.security.util.ProxiedEntityUtils;
import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.audit.Auditor;
import datawave.webservice.query.cachedresults.CacheableQueryRow;
import datawave.webservice.query.data.ObjectSizeOf;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.NoResultsQueryException;
import datawave.webservice.query.exception.NotFoundQueryException;
import datawave.webservice.query.exception.PreConditionFailedQueryException;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.exception.QueryExceptionType;
import datawave.webservice.query.exception.UnauthorizedQueryException;
import datawave.webservice.query.result.EdgeQueryResponseBase;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.CachedResultsDescribeResponse;
import datawave.webservice.result.CachedResultsResponse;
import datawave.webservice.result.EventQueryResponseBase;
import datawave.webservice.result.GenericResponse;
import datawave.webservice.result.TotalResultsAware;
import datawave.webservice.result.VoidResponse;

@Service
@ConditionalOnProperty(name = "datawave.query.cached-results.enabled", havingValue = "true", matchIfMissing = true)
public class CachedResultsQueryService {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    public static final String TABLE_PLACEHOLDER = "%TABLE%";
    private static final String VIEW_PLACEHOLDER = "%VIEW%";
    private static final String TABLE_COLS_PLACEHOLDER = "%TABLE_COLS%";
    private static final String VIEW_COLS_PLACEHOLDER = "%VIEW_COLS%";
    private static final String FIELD_DEFINITIONS_PLACEHOLDER = "%FIELD_DEFINITIONS%";
    private static final String PREPARED_FIELDS_PLACEHOLDER = "%PREPARED_FIELDS%";
    private static final String PREPARED_VALUES_PLACEHOLDER = "%PREPARED_VALUES%";
    private static final String FIELD = "field";
    private static final String BACKTICK = "`";
    private static final String SPACE = " ";
    private static final String LPAREN = "(";
    private static final String RPAREN = ")";
    
    private final CachedResultsQueryProperties cachedResultsQueryProperties;
    private final JdbcTemplate cachedResultsJdbcTemplate;
    private final CachedResultsQueryCache cachedResultsQueryCache;
    private final QueryService queryService;
    private final AuditClient auditClient;
    // Note: SecurityMarking needs to be request scoped
    private final RequestScopeBeanSupplier<SecurityMarking> scopedSecurityMarking;
    private final QueryLogicFactory queryLogicFactory;
    private final QueryStorageCache queryStorageCache;
    private final ResponseObjectFactory responseObjectFactory;
    private final MarkingFunctions markingFunctions;
    // Note: CachedResultsQueryParameters needs to be request scoped
    private final RequestScopeBeanSupplier<CachedResultsQueryParameters> scopedCachedResultsQueryParameters;
    private final String fieldDefinitions;
    private final String preparedFields;
    private final String preparedValues;
    private final ExecutorService executorService = Executors.newCachedThreadPool();
    
    public CachedResultsQueryService(CachedResultsQueryProperties cachedResultsQueryProperties, JdbcTemplate cachedResultsJdbcTemplate,
                    CachedResultsQueryCache cachedResultsQueryCache, QueryService queryService, AuditClient auditClient, SecurityMarking securityMarking,
                    QueryLogicFactory queryLogicFactory, QueryStorageCache queryStorageCache, ResponseObjectFactory responseObjectFactory,
                    MarkingFunctions markingFunctions, CachedResultsQueryParameters cachedResultsQueryParameters) {
        this.cachedResultsQueryProperties = cachedResultsQueryProperties;
        this.cachedResultsJdbcTemplate = cachedResultsJdbcTemplate;
        this.cachedResultsQueryCache = cachedResultsQueryCache;
        this.queryService = queryService;
        this.auditClient = auditClient;
        this.scopedSecurityMarking = new RequestScopeBeanSupplier<>(securityMarking);
        this.queryLogicFactory = queryLogicFactory;
        this.queryStorageCache = queryStorageCache;
        this.responseObjectFactory = responseObjectFactory;
        this.markingFunctions = markingFunctions;
        this.scopedCachedResultsQueryParameters = new RequestScopeBeanSupplier<>(cachedResultsQueryParameters);
        this.fieldDefinitions = IntStream.range(0, cachedResultsQueryProperties.getNumFields()).mapToObj(x -> FIELD + x + " LONGTEXT")
                        .collect(Collectors.joining(", "));
        this.preparedFields = IntStream.range(0, cachedResultsQueryProperties.getNumFields()).mapToObj(x -> FIELD + x).collect(Collectors.joining(", "));
        this.preparedValues = Stream.generate(() -> "?").limit(cachedResultsQueryProperties.getNumFields()).collect(Collectors.joining(", "));
        initializeTableTemplate();
    }
    
    private void initializeTableTemplate() {
        // @formatter:off
        String createTableTemplate = cachedResultsQueryProperties.getStatementTemplates().getCreateTableTemplate()
                .replace(FIELD_DEFINITIONS_PLACEHOLDER, fieldDefinitions);
        // @formatter:on
        try {
            cachedResultsJdbcTemplate.execute(createTableTemplate);
        } catch (DataAccessException e) {
            log.error("Unable to create 'create table' template with statement: {}", createTableTemplate, e);
            throw e;
        }
    }
    
    /**
     * Creates the specified query, runs it to completion, and caches the results in SQL
     *
     * The query represented by queryId will be duplicated, and it's results will be written to SQL.
     * 
     * @param definedQueryId
     *            the query id of the defined query, not null
     * @param alias
     *            an optional, user-defined alias which can be used to reference the query
     * @param currentUser
     *            the current user, not null
     * @return the view name
     * @throws QueryException
     *             if the operation fails
     */
    public GenericResponse<String> load(String definedQueryId, String alias, ProxiedUserDetails currentUser) throws QueryException {
        log.info("Request: {}/load from {} with alias: {}", definedQueryId, ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName()), alias);
        
        CachedResultsQueryStatus cachedResultsQueryStatus = null;
        try {
            // was load already called for this query id? if so, stop
            boolean lockAcquired = cachedResultsQueryCache.tryLockQueryStatus(definedQueryId, TimeUnit.SECONDS.toMillis(30));
            try {
                if (lockAcquired) {
                    // get the cached results status for the query
                    cachedResultsQueryStatus = cachedResultsQueryCache.getQueryStatus(definedQueryId);
                    
                    // if a cached results query doesn't already exist, create one
                    if (cachedResultsQueryStatus == null) {
                        cachedResultsQueryStatus = cachedResultsQueryCache.createQuery(definedQueryId, null, alias, currentUser);
                        if (alias != null) {
                            cachedResultsQueryCache.putQueryIdByAliasLookup(cachedResultsQueryStatus.getAlias(), definedQueryId);
                        }
                    } else if (cachedResultsQueryStatus.getState() == NONE) {
                        // cachedResultsQueryStatus may have been set to NONE by loadAndCreateAsync
                        cachedResultsQueryStatus.setState(LOADING);
                    } else {
                        // otherwise if a cached results query already exists then stop
                        if (cachedResultsQueryStatus.getState() == FAILED) {
                            log.warn("The cached results query for {} has FAILED", definedQueryId);
                        } else {
                            log.info("A cached results query for {} is {}", definedQueryId, cachedResultsQueryStatus.getState());
                        }
                        throw new QueryException(DatawaveErrorCode.QUERY_LOCKED_ERROR);
                    }
                } else {
                    log.error("Unable obtain lock on query {}", definedQueryId);
                    throw new QueryException(DatawaveErrorCode.QUERY_LOCKED_ERROR);
                }
            } finally {
                if (cachedResultsQueryStatus != null) {
                    try {
                        cachedResultsQueryCache.update(definedQueryId, cachedResultsQueryStatus);
                    } catch (Exception e) {
                        log.error("Unable to update query cache", e);
                    }
                }
                if (lockAcquired) {
                    cachedResultsQueryCache.unlockQueryStatus(definedQueryId);
                }
            }
            
            // run the query and cache the results
            // this could take a while!
            load(cachedResultsQueryStatus);
            
            // set the state based on that status of the running query
            QueryStatus runningQueryStatus = queryStorageCache.getQueryStatus(cachedResultsQueryStatus.getRunningQueryId());
            switch (runningQueryStatus.getQueryState()) {
                case CANCEL:
                    cachedResultsQueryStatus.setState(CANCELED);
                    throw new QueryException("Load operation failed because query was canceled");
                case FAIL:
                    cachedResultsQueryStatus.setState(FAILED);
                    throw new QueryException("Load operation failed because query failed");
                default:
                    cachedResultsQueryStatus.setState(LOADED);
            }
            
            // create the view
            cachedResultsQueryStatus.setView(getViewName(cachedResultsQueryStatus.getRunningQueryId()));
            createView(cachedResultsQueryStatus.getTableName(), cachedResultsQueryStatus.getView(), cachedResultsQueryStatus.getFieldIndexMap());
            
            // add an alternate lookup path for the view
            cachedResultsQueryCache.putQueryIdByViewLookup(cachedResultsQueryStatus.getView(), definedQueryId);
            
            // return the view name in the response
            GenericResponse<String> response = new GenericResponse<>();
            response.setResult(cachedResultsQueryStatus.getView());
            return response;
        } catch (Exception e) {
            // Delete the sql table and view
            if (cachedResultsQueryStatus != null) {
                if (cachedResultsQueryStatus.getTableName() != null) {
                    dropTable(cachedResultsQueryStatus.getTableName());
                }
                
                if (cachedResultsQueryStatus.getView() != null) {
                    dropView(cachedResultsQueryStatus.getView());
                }
            }
            
            if (cachedResultsQueryStatus != null && cachedResultsQueryStatus.getState() == LOADING) {
                cachedResultsQueryStatus.setState(FAILED);
            }
            throw e instanceof QueryException ? (QueryException) e : new QueryException(e);
        } finally {
            if (cachedResultsQueryStatus != null) {
                try {
                    cachedResultsQueryCache.update(definedQueryId, cachedResultsQueryStatus);
                } catch (InterruptedException e) {
                    log.error("Unable to update query cache", e);
                }
            }
        }
    }
    
    private void load(CachedResultsQueryStatus cachedResultsQueryStatus) throws QueryException {
        boolean queryClosed = false;
        
        try {
            // duplicate the query
            GenericResponse<?> duplicateResponse = queryService.duplicate(cachedResultsQueryStatus.getDefinedQueryId(),
                            cachedResultsQueryStatus.getCurrentUser());
            
            String runningQueryId = (String) duplicateResponse.getResult();
            
            // if duplicate was successful, then the user owns this query - no need to validate ownership
            // get the query logic name
            QueryStatus queryStatus = queryStorageCache.getQueryStatus(runningQueryId);
            if (queryStatus != null) {
                cachedResultsQueryStatus.setQueryLogicName(queryStatus.getQuery().getQueryLogicName());
                cachedResultsQueryStatus.setOrigQuery(queryStatus.getQuery().getQuery());
            } else {
                log.error("Query for {} does not exist", cachedResultsQueryStatus.getDefinedQueryId());
                throw new BadRequestQueryException("Query for " + cachedResultsQueryStatus.getDefinedQueryId() + "does not exist");
            }
            
            // get the query logic and make sure that it's cacheable. if not, close and remove the query
            CacheableLogic cacheableLogic;
            QueryLogic<?> queryLogic = queryLogicFactory.getQueryLogic(cachedResultsQueryStatus.getQueryLogicName(), cachedResultsQueryStatus.getCurrentUser());
            Transformer<?,?> transformer = queryLogic.getTransformer(queryStatus.getQuery());
            if (transformer instanceof CacheableLogic) {
                cacheableLogic = (CacheableLogic) transformer;
            } else {
                try {
                    queryService.close(runningQueryId, cachedResultsQueryStatus.getCurrentUser());
                    queryService.remove(runningQueryId, cachedResultsQueryStatus.getCurrentUser());
                } catch (QueryException e) {
                    log.error("Could not close or remove query: " + runningQueryId, e);
                }
                
                log.error("Cannot load results for a query logic that is not cacheable: {}", cachedResultsQueryStatus.getQueryLogicName());
                throw new BadRequestQueryException(
                                "Cannot load results for a query logic that is not cacheable: " + cachedResultsQueryStatus.getQueryLogicName());
            }
            
            cachedResultsQueryStatus.setQuery(queryStatus.getQuery());
            
            // store the running query id in the cache
            cachedResultsQueryStatus.setRunningQueryId(runningQueryId);
            
            // create the SQL table
            cachedResultsQueryStatus.setTableName(getTableName(cachedResultsQueryStatus.getRunningQueryId()));
            createTable(cachedResultsQueryStatus.getTableName());
            
            // before we load the results, update the cached query status
            cachedResultsQueryCache.update(cachedResultsQueryStatus.getDefinedQueryId(), cachedResultsQueryStatus);
            
            // save the field index map in the cache
            cachedResultsQueryStatus.setFieldIndexMap(new HashMap<>());
            
            // get all of the pages and load the results into SQL
            boolean done = false;
            do {
                // request the next page
                BaseQueryResponse nextResponse = null;
                try {
                    nextResponse = queryService.next(cachedResultsQueryStatus.getRunningQueryId(), cachedResultsQueryStatus.getCurrentUser());
                } catch (NoResultsQueryException e) {
                    // the query is closed automatically if we exhaust the results.
                    queryClosed = true;
                } catch (QueryException e) {
                    log.error("Encountered an unexpected error calling next for {}", cachedResultsQueryStatus.getRunningQueryId());
                    throw e;
                }
                
                if (nextResponse != null) {
                    List<QueryExceptionType> exceptions = nextResponse.getExceptions();
                    if (nextResponse.getExceptions() != null && !nextResponse.getExceptions().isEmpty()) {
                        throw new RuntimeException(exceptions.get(0).getMessage());
                    }
                    
                    // convert the response to a list of cacheable query rows
                    List<CacheableQueryRow> cacheableQueryRows = new ArrayList<>();
                    for (Object result : getResults(nextResponse)) {
                        cacheableQueryRows.add(cacheableLogic.writeToCache(result));
                    }
                    
                    // load the cacheable query rows into the SQL table
                    if (!cacheableQueryRows.isEmpty()) {
                        loadCacheableQueryRows(cachedResultsQueryStatus, cacheableQueryRows);
                    } else {
                        done = true;
                    }
                } else {
                    done = true;
                    
                    // Dump the fieldMap for debugging
                    if (log.isTraceEnabled()) {
                        for (Map.Entry<String,Integer> e : cachedResultsQueryStatus.getFieldIndexMap().entrySet()) {
                            log.trace("Field mapping: {} -> {}", e.getKey(), e.getValue());
                        }
                    }
                }
            } while (!done);
        } catch (Exception e) {
            log.error("Encountered unknown error loading query results for {}", cachedResultsQueryStatus.getDefinedQueryId(), e);
            throw e instanceof QueryException ? (QueryException) e
                            : new QueryException("Encountered unknown error loading query results for " + cachedResultsQueryStatus.getDefinedQueryId(), e);
        } finally {
            if (!queryClosed && cachedResultsQueryStatus.getRunningQueryId() != null) {
                try {
                    queryService.close(cachedResultsQueryStatus.getRunningQueryId(), cachedResultsQueryStatus.getCurrentUser());
                } catch (QueryException e) {
                    // this may happen if the query has already been automatically closed
                    log.warn("Encountered error while closing query {}: {}", cachedResultsQueryStatus.getRunningQueryId(), e.getMessage());
                }
            }
        }
    }
    
    protected List<?> getResults(BaseQueryResponse nextResponse) {
        if (nextResponse instanceof EventQueryResponseBase) {
            return ((EventQueryResponseBase) nextResponse).getEvents();
        } else if (nextResponse instanceof EdgeQueryResponseBase) {
            return ((EdgeQueryResponseBase) nextResponse).getEdges();
        } else {
            throw new IllegalStateException("incompatible response");
        }
    }
    
    private void loadCacheableQueryRows(CachedResultsQueryStatus cachedResultsQueryStatus, List<CacheableQueryRow> cacheableQueryRows) {
        // use the prepared insert statement to write all the values
        // @formatter:off
        String insert = cachedResultsQueryProperties.getStatementTemplates().getInsert()
                .replace(TABLE_PLACEHOLDER, cachedResultsQueryStatus.getTableName())
                .replace(PREPARED_FIELDS_PLACEHOLDER, preparedFields)
                .replace(PREPARED_VALUES_PLACEHOLDER, preparedValues);
        // @formatter:on
        
        int attempt = 0;
        boolean success = false;
        while (!success && attempt < cachedResultsQueryProperties.getMaxInsertAttempts()) {
            
            // determine the maximum value length
            // (maximum value length progressively shrinks based on the number of attempts)
            final int maxValueLength = (int) (cachedResultsQueryProperties.getMaxValueLength()
                            * ((double) (cachedResultsQueryProperties.getMaxInsertAttempts() - attempt) / cachedResultsQueryProperties.getMaxInsertAttempts()));
            try {
                cachedResultsJdbcTemplate.batchUpdate(insert, new BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement ps, int i) throws SQLException {
                        CacheableQueryRow cacheableQueryRow = cacheableQueryRows.get(i);
                        
                        // Each entry is a different visibility.
                        ps.setString(1, cachedResultsQueryStatus.getCurrentUser().getShortName());
                        ps.setString(2, cachedResultsQueryStatus.getDefinedQueryId());
                        ps.setString(3, cachedResultsQueryStatus.getQueryLogicName());
                        ps.setString(4, cacheableQueryRow.getDataType());
                        ps.setString(5, cacheableQueryRow.getEventId());
                        ps.setString(6, cacheableQueryRow.getRow());
                        ps.setString(7, cacheableQueryRow.getColFam());
                        ps.setString(8, MarkingFunctions.Encoding.toString(new TreeMap<>(cacheableQueryRow.getMarkings())));
                        
                        // keep track of the populated columns
                        Set<Integer> populatedColumns = new HashSet<>();
                        for (Map.Entry<String,String> entry : cacheableQueryRow.getColumnValues().entrySet()) {
                            String columnName = entry.getKey();
                            String columnValue = entry.getValue();
                            
                            // Get the field number from the fieldMap.
                            Integer columnNumber = cachedResultsQueryStatus.getFieldIndexMap().computeIfAbsent(columnName,
                                            k -> CacheableQueryRow.getFixedColumnSet().size() + cachedResultsQueryStatus.getFieldIndexMap().size() + 1);
                            
                            if (columnValue.length() > maxValueLength) {
                                String truncated = "<truncated>";
                                columnValue = columnValue.substring(0, maxValueLength - truncated.length()) + truncated;
                                ps.setString(columnNumber, columnValue);
                            } else {
                                ps.setString(columnNumber, columnValue);
                            }
                            
                            // keep track of which columns are/aren't populated
                            populatedColumns.add(columnNumber);
                            
                            if (log.isTraceEnabled()) {
                                log.trace("Set parameter: {} with field name: {} to value: {}", columnNumber, columnName, columnValue);
                            }
                        }
                        
                        ps.setString(9, cacheableQueryRow.getColumnSecurityMarkingString(cachedResultsQueryStatus.getFieldIndexMap()));
                        ps.setString(10, cacheableQueryRow.getColumnTimestampString(cachedResultsQueryStatus.getFieldIndexMap()));
                        
                        // need to set all the unset columns to NULL
                        int beginColumn = CacheableQueryRow.getFixedColumnSet().size() + 1;
                        int endColumn = CacheableQueryRow.getFixedColumnSet().size() + cachedResultsQueryProperties.getNumFields();
                        for (int columnIndex = beginColumn; columnIndex <= endColumn; columnIndex++) {
                            if (!populatedColumns.contains(columnIndex)) {
                                ps.setNull(columnIndex, Types.VARCHAR);
                            }
                        }
                    }
                    
                    @Override
                    public int getBatchSize() {
                        return cacheableQueryRows.size();
                    }
                });
                
                success = true;
                
                // update the total number of rows written to sql
                cachedResultsQueryStatus.setRowsWritten(cachedResultsQueryStatus.getRowsWritten() + cacheableQueryRows.size());
            } catch (DataAccessException e) {
                if (++attempt == cachedResultsQueryProperties.getMaxInsertAttempts()) {
                    log.error("Batch write FAILED for query {}", cachedResultsQueryStatus.getRunningQueryId(), e);
                    throw e;
                } else {
                    log.warn("Batch write FAILED for query {}", cachedResultsQueryStatus.getRunningQueryId(), e);
                }
            }
        }
    }
    
    private void createTable(String tableName) throws DataAccessException {
        // @formatter:off
        String createTable = cachedResultsQueryProperties.getStatementTemplates().getCreateTable()
                .replace(TABLE_PLACEHOLDER, tableName);
        // @formatter:on
        
        try {
            cachedResultsJdbcTemplate.execute(createTable);
        } catch (DataAccessException e) {
            log.error("Unable to create table {} using statement {}", tableName, createTable, e);
            throw e;
        }
    }
    
    private void dropTable(String tableName) {
        String statement = cachedResultsQueryProperties.getStatementTemplates().getDropTable().replace(TABLE_PLACEHOLDER, tableName);
        try {
            cachedResultsJdbcTemplate.execute(statement);
        } catch (Exception e) {
            log.error("Unable to drop table {} using statement {}", tableName, statement, e);
        }
    }
    
    private void createView(String tableName, String viewName, Map<String,Integer> fieldIndexMap) {
        String baseCols = String.join(",", CacheableQueryRow.getFixedColumnSet());
        StringBuilder viewCols = new StringBuilder();
        StringBuilder tableCols = new StringBuilder();
        viewCols.append(baseCols);
        tableCols.append(baseCols);
        for (Map.Entry<String,Integer> entry : fieldIndexMap.entrySet()) {
            viewCols.append(",").append("`").append(entry.getKey()).append("`");
            tableCols.append(",").append(FIELD).append(entry.getValue() - CacheableQueryRow.getFixedColumnSet().size() - 1);
        }
        
        // @formatter:off
        String createView = cachedResultsQueryProperties.getStatementTemplates().getCreateView()
                .replace(TABLE_PLACEHOLDER, tableName)
                .replace(VIEW_PLACEHOLDER, viewName)
                .replace(TABLE_COLS_PLACEHOLDER, tableCols)
                .replace(VIEW_COLS_PLACEHOLDER, viewCols);
        // @formatter:on
        
        try {
            cachedResultsJdbcTemplate.execute(createView);
        } catch (DataAccessException e) {
            log.error("Unable to create view {} using statement {}", tableName, createView, e);
            throw e;
        }
    }
    
    private void dropView(String viewName) {
        String statement = cachedResultsQueryProperties.getStatementTemplates().getDropView().replace(VIEW_PLACEHOLDER, viewName);
        try {
            cachedResultsJdbcTemplate.execute(statement);
        } catch (Exception e) {
            log.error("Unable to drop table {} using statement {}", viewName, statement, e);
        }
    }
    
    private String getTableName(String newQueryId) {
        return "t" + newQueryId.replace("-", "");
    }
    
    private String getViewName(String newQueryId) {
        return "v" + newQueryId.replace("-", "");
    }
    
    public CachedResultsResponse create(String key, MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser) throws QueryException {
        String user = ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName());
        if (log.isDebugEnabled()) {
            log.info("Request: {}/create from {} with params: {}", key, user, parameters);
        } else {
            log.info("Request: {}/create from {}", key, user);
        }
        
        try {
            // make sure the query is valid, and the user can act on it
            CachedResultsQueryStatus cachedResultsQueryStatus = validateRequest(key, currentUser);
            
            if (cachedResultsQueryStatus.getState() == LOADED) {
                if (cachedResultsQueryStatus.getDefinedQueryId() != null) {
                    parameters.computeIfAbsent(CachedResultsQueryParameters.QUERY_ID,
                                    s -> Collections.singletonList(cachedResultsQueryStatus.getDefinedQueryId()));
                }
                if (cachedResultsQueryStatus.getAlias() != null) {
                    parameters.computeIfAbsent(CachedResultsQueryParameters.ALIAS, s -> Collections.singletonList(cachedResultsQueryStatus.getAlias()));
                }
                if (cachedResultsQueryStatus.getView() != null) {
                    parameters.computeIfAbsent(CachedResultsQueryParameters.VIEW, s -> Collections.singletonList(cachedResultsQueryStatus.getView()));
                }
                return create(cachedResultsQueryStatus.getDefinedQueryId(), parameters);
            } else {
                throw new BadRequestQueryException("Cannot call create on a query that has not finished loading", HttpStatus.SC_BAD_REQUEST + "-1");
            }
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            QueryException queryException = new QueryException(DatawaveErrorCode.QUERY_CLOSE_ERROR, e, "Unknown error creating cached results query " + key);
            log.error("Unknown error creating cached results query {}", key, queryException);
            throw queryException;
        }
    }
    
    private CachedResultsResponse create(String definedQueryId, MultiValueMap<String,String> parameters)
                    throws QueryException, InterruptedException, CloneNotSupportedException {
        CachedResultsQueryParameters cachedResultsQueryParameters = scopedCachedResultsQueryParameters.get();
        cachedResultsQueryParameters.validate(parameters);
        
        // mark the query as CREATING
        CachedResultsQueryStatus cachedResultsQueryStatus = cachedResultsQueryCache.lockedUpdate(definedQueryId, status -> {
            if (status.getState() == LOADED) {
                status.setState(CREATING);
            } else {
                throw new BadRequestQueryException("Cannot call create on a query that is not loaded", HttpStatus.SC_BAD_REQUEST + "-1");
            }
        });
        
        // this will allow cachedResultsQueryStatus to be accessed by the cachedQueryId
        cachedResultsQueryCache.putQueryIdByCachedQueryIdLookup(cachedResultsQueryParameters.getQueryId(), definedQueryId);
        cachedResultsQueryStatus.setCachedQueryId(cachedResultsQueryParameters.getQueryId());
        
        // this will allow cachedResultsQueryStatus to be accessed by the alias
        if (cachedResultsQueryParameters.getAlias() != null) {
            cachedResultsQueryCache.putQueryIdByAliasLookup(cachedResultsQueryParameters.getAlias(), definedQueryId);
            cachedResultsQueryStatus.setAlias(cachedResultsQueryParameters.getAlias());
        }
        
        if (cachedResultsQueryParameters.getPagesize() <= 0) {
            cachedResultsQueryParameters.setPagesize(cachedResultsQueryProperties.getDefaultPageSize());
        }
        
        int maxPageSize = cachedResultsQueryProperties.getMaxPageSize();
        if (maxPageSize > 0 && cachedResultsQueryParameters.getPagesize() > maxPageSize) {
            throw new PreConditionFailedQueryException(DatawaveErrorCode.REQUESTED_PAGE_SIZE_TOO_LARGE,
                            MessageFormat.format("{0} > {1}.", cachedResultsQueryParameters.getPagesize(), maxPageSize));
        }
        
        Set<String> fixedFields = new HashSet<>();
        if (!StringUtils.isEmpty(cachedResultsQueryParameters.getFixedFields())) {
            fixedFields = new HashSet<>();
            for (String field : cachedResultsQueryParameters.getFixedFields().split(",")) {
                fixedFields.add(field.trim());
            }
        }
        
        // save the params in the status
        cachedResultsQueryStatus.setFields(cachedResultsQueryParameters.getFields());
        cachedResultsQueryStatus.setConditions(cachedResultsQueryParameters.getConditions());
        cachedResultsQueryStatus.setGrouping(cachedResultsQueryParameters.getGrouping());
        cachedResultsQueryStatus.setOrder(cachedResultsQueryParameters.getOrder());
        cachedResultsQueryStatus.setPageSize(cachedResultsQueryParameters.getPagesize());
        cachedResultsQueryStatus.setFixedFields(fixedFields);
        
        // generate the sql query
        cachedResultsQueryStatus.setSqlQuery(generateSqlQuery(cachedResultsQueryStatus));
        
        QueryLogic<?> queryLogic = queryLogicFactory.getQueryLogic(cachedResultsQueryStatus.getQueryLogicName(), cachedResultsQueryStatus.getCurrentUser());
        if (queryLogic == null) {
            log.error("Could not find description for logic {}", cachedResultsQueryStatus.getQueryLogicName());
            throw new QueryException(DatawaveErrorCode.QUERY_LOGIC_ERROR,
                            "Could not find description for logic " + cachedResultsQueryStatus.getQueryLogicName());
        }
        
        // audit the query again
        // @formatter:off
        audit(cachedResultsQueryStatus.getRunningQueryId(),
                queryLogic.getAuditType(),
                queryLogic.getLogicName(),
                cachedResultsQueryStatus.getOrigQuery(),
                cachedResultsQueryStatus.getSqlQuery(),
                createAuditParameters(parameters, cachedResultsQueryStatus),
                cachedResultsQueryStatus.getCurrentUser());
        // @formatter:on
        
        // mark the query as CREATED
        cachedResultsQueryStatus.setState(CREATED);
        cachedResultsQueryCache.update(definedQueryId, cachedResultsQueryStatus);
        
        CachedResultsResponse response = new CachedResultsResponse();
        response.setAlias(cachedResultsQueryStatus.getAlias());
        response.setOriginalQueryId(cachedResultsQueryStatus.getDefinedQueryId());
        response.setQueryId(cachedResultsQueryStatus.getCachedQueryId());
        response.setViewName(cachedResultsQueryStatus.getView());
        response.setTotalRows(cachedResultsQueryStatus.getRowsWritten());
        return response;
    }
    
    public CachedResultsResponse loadAndCreate(String definedQueryId, MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser)
                    throws QueryException {
        String user = ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName());
        if (log.isDebugEnabled()) {
            log.info("Request: {}/loadAndCreate from {} with params: {}", definedQueryId, user, parameters);
        } else {
            log.info("Request: {}/loadAndCreate from {}", definedQueryId, user);
        }
        
        String alias = parameters.getFirst(CachedResultsQueryParameters.ALIAS);
        GenericResponse<String> loadResponse = load(definedQueryId, alias, currentUser);
        return create(loadResponse.getResult(), parameters, currentUser);
    }
    
    public VoidResponse loadAndCreateAsync(String definedQueryId, MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser,
                    CachedResultsQueryParameters threadCachedResultsQueryParameters, SecurityMarking threadSecurityMarking) {
        VoidResponse response = new VoidResponse();
        try {
            String queryId = parameters.getFirst(CachedResultsQueryParameters.QUERY_ID);
            if (queryId == null) {
                throw new BadRequestQueryException(DatawaveErrorCode.MISSING_REQUIRED_PARAMETER, "queryId can not be null");
            }
            String alias = parameters.getFirst(CachedResultsQueryParameters.ALIAS);
            // get the cached results status for the query
            CachedResultsQueryStatus crqStatus = cachedResultsQueryCache.getQueryStatus(definedQueryId);
            if (crqStatus != null) {
                // if a cached results query already exists then stop
                if (crqStatus.getState() == FAILED) {
                    log.warn("The cached results query for {} has FAILED", definedQueryId);
                } else {
                    log.info("A cached results query for {} is {}", definedQueryId, crqStatus.getState());
                }
                throw new QueryException(DatawaveErrorCode.QUERY_LOCKED_ERROR);
            }
            
            cachedResultsQueryCache.createQuery(definedQueryId, queryId, alias, currentUser);
            cachedResultsQueryCache.lockedUpdate(definedQueryId, cachedResultsQueryStatus -> {
                // this allows load() to differentiate between allowed loadAndCreateAsync calls (status == NONE) and
                // disallowed duplicate load() calls (cachedResultsQueryStatus already exists and status != NONE)
                cachedResultsQueryStatus.setState(NONE);
                
                // this will allow cachedResultsQueryStatus to be accessed by the cachedQueryId
                cachedResultsQueryCache.putQueryIdByCachedQueryIdLookup(queryId, definedQueryId);
                cachedResultsQueryStatus.setCachedQueryId(queryId);
                
                if (alias != null) {
                    // this will allow cachedResultsQueryStatus to be accessed by the alias
                    cachedResultsQueryCache.putQueryIdByAliasLookup(alias, definedQueryId);
                    cachedResultsQueryStatus.setAlias(alias);
                }
            });
            
            executorService.submit(() -> {
                try {
                    scopedCachedResultsQueryParameters.getThreadLocalOverride().set(threadCachedResultsQueryParameters);
                    scopedSecurityMarking.getThreadLocalOverride().set(threadSecurityMarking);
                    loadAndCreate(definedQueryId, parameters, currentUser);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                    cachedResultsQueryCache.removeQueryStatus(definedQueryId);
                } finally {
                    scopedCachedResultsQueryParameters.getThreadLocalOverride().remove();
                    scopedSecurityMarking.getThreadLocalOverride().remove();
                }
            });
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            response.addException(e);
        }
        return response;
    }
    
    private MultiValueMap<String,String> createAuditParameters(CachedResultsQueryStatus cachedResultsQueryStatus) {
        return createAuditParameters(null, cachedResultsQueryStatus);
    }
    
    protected MultiValueMap<String,String> createAuditParameters(MultiValueMap<String,String> parameters, CachedResultsQueryStatus cachedResultsQueryStatus) {
        MultiValueMap<String,String> auditParameters = new LinkedMultiValueMap<>();
        
        if (parameters != null) {
            auditParameters.addAll(parameters);
        }
        
        auditParameters.add(QUERY_VISIBILITY, cachedResultsQueryStatus.getQuery().getColumnVisibility());
        return auditParameters;
    }
    
    public void audit(String auditId, Auditor.AuditType auditType, String logicName, String origQuery, String sqlQuery, MultiValueMap<String,String> parameters,
                    ProxiedUserDetails currentUser) throws BadRequestQueryException {
        
        // if we haven't already, validate the markings
        SecurityMarking securityMarking = scopedSecurityMarking.get();
        if (securityMarking.toColumnVisibilityString() == null) {
            validateSecurityMarkings(parameters);
        }
        
        // set some audit parameters which are used internally
        setInternalAuditParameters(logicName, currentUser.getPrimaryUser().getDn().subjectDN(), parameters);
        
        parameters.add(PrivateAuditConstants.AUDIT_TYPE, auditType.name());
        if (auditType != Auditor.AuditType.NONE) {
            // audit the query before execution
            try {
                // is the user didn't set an audit id, use the query id
                if (!parameters.containsKey(AuditParameters.AUDIT_ID)) {
                    parameters.set(AuditParameters.AUDIT_ID, auditId);
                }
                
                String query = "User running secondary query on cached results of original query, original query: " + origQuery + ", secondary query: "
                                + sqlQuery;
                
                // @formatter:off
                AuditClient.Request auditRequest = new AuditClient.Request.Builder()
                        .withParams(parameters)
                        .withQueryExpression(query)
                        .withDatawaveUserDetails(new DatawaveUserDetails(currentUser.getProxiedUsers()))
                        .withMarking(securityMarking)
                        .withAuditType(Auditor.AuditType.PASSIVE)
                        .withQueryLogic(logicName)
                        .build();
                // @formatter:on
                
                log.info("[{}] Sending audit request with parameters {}", auditId, auditRequest);
                
                auditClient.submit(auditRequest);
            } catch (IllegalArgumentException e) {
                log.error("Error validating audit parameters", e);
                throw new BadRequestQueryException(DatawaveErrorCode.MISSING_REQUIRED_PARAMETER, e);
            } catch (Exception e) {
                log.error("Error auditing query", e);
                throw new BadRequestQueryException(DatawaveErrorCode.QUERY_AUDITING_ERROR, e);
            }
        }
    }
    
    public void setInternalAuditParameters(String queryLogicName, String userDn, MultiValueMap<String,String> parameters) {
        // Set private audit-related parameters, stripping off any that the user might have passed in first.
        // These are parameters that aren't passed in by the user, but rather are computed from other sources.
        PrivateAuditConstants.stripPrivateParameters(parameters);
        parameters.add(PrivateAuditConstants.LOGIC_CLASS, queryLogicName);
        parameters.set(PrivateAuditConstants.COLUMN_VISIBILITY, scopedSecurityMarking.get().toColumnVisibilityString());
        parameters.add(PrivateAuditConstants.USER_DN, userDn);
    }
    
    public void validateSecurityMarkings(MultiValueMap<String,String> parameters) throws BadRequestQueryException {
        try {
            scopedSecurityMarking.get().validate(parameters);
        } catch (IllegalArgumentException e) {
            log.error("Failed security markings validation", e);
            throw new BadRequestQueryException(DatawaveErrorCode.SECURITY_MARKING_CHECK_ERROR, e);
        }
    }
    
    public String generateSqlQuery(CachedResultsQueryStatus cachedResultsQueryStatus) {
        CachedResultsQueryParameters.validate(cachedResultsQueryStatus.getView());
        StringBuilder buf = new StringBuilder();
        
        String fields = "*";
        if (!StringUtils.isEmpty(StringUtils.trimToNull(cachedResultsQueryStatus.getFields()))) {
            fields = cachedResultsQueryStatus.getFields();
        }
        
        String conditions = null;
        if (!StringUtils.isEmpty(StringUtils.trimToNull(cachedResultsQueryStatus.getConditions()))) {
            conditions = cachedResultsQueryStatus.getConditions();
        }
        
        String order = null;
        if (!StringUtils.isEmpty(StringUtils.trimToNull(cachedResultsQueryStatus.getOrder()))) {
            order = cachedResultsQueryStatus.getOrder();
        }
        
        String grouping = null;
        if (!StringUtils.isEmpty(StringUtils.trimToNull(cachedResultsQueryStatus.getGrouping()))) {
            grouping = cachedResultsQueryStatus.getGrouping();
        }
        
        Set<String> viewColumnNames = cachedResultsQueryStatus.getFieldIndexMap().keySet();
        
        if (!fields.equals("*")) {
            LinkedHashSet<String> fieldSet = new LinkedHashSet<>();
            String[] result = tokenizeOutsideParens(fields, ',');
            
            LinkedHashSet<String> requestedFieldSet = new LinkedHashSet<>();
            
            for (String s : result) {
                s = s.replace("`", "").trim();
                s = quoteField(s, viewColumnNames);
                requestedFieldSet.add(s);
            }
            
            if (requestedFieldSet.contains("*")) {
                // make sure that * is in front
                requestedFieldSet.remove("*");
                fieldSet.add("*");
            } else {
                // make sure that all fixed columns are included in the SELECT
                fieldSet.addAll(CacheableQueryRow.getFixedColumnSet());
            }
            fieldSet.addAll(requestedFieldSet);
            
            fields = StringUtils.join(fieldSet, ",");
        }
        
        if (null != conditions) {
            // quote fields that are known columns
            StringBuilder newConditions = new StringBuilder();
            String[] conditionsSplit = conditions.split(" ");
            for (String s : conditionsSplit) {
                String field = s.replace("`", "").trim();
                if (cachedResultsQueryStatus.getFieldIndexMap().containsKey(field) || isFunction(field)) {
                    newConditions.append(quoteField(field, viewColumnNames)).append(SPACE);
                } else {
                    newConditions.append(s).append(SPACE);
                }
            }
            if (newConditions.toString().trim().isEmpty()) {
                conditions = null;
            } else {
                conditions = newConditions.toString().trim();
            }
        }
        
        order = buildOrderClause(order, viewColumnNames);
        
        if (null != grouping) {
            // quote fields in the group by
            List<String> newGroup = new ArrayList<>();
            String[] groupSplit = grouping.split(",");
            for (String s : groupSplit) {
                s = s.replace("`", "").trim();
                // add quoted field
                newGroup.add(quoteField(s, viewColumnNames));
            }
            if (newGroup.isEmpty()) {
                grouping = null;
            } else {
                grouping = StringUtils.join(newGroup, ",");
            }
            
        }
        
        buf.append("SELECT ").append(fields).append(" FROM ").append(cachedResultsQueryStatus.getView());
        
        String user = cachedResultsQueryStatus.getCurrentUser().getShortName();
        if (conditions == null || conditions.isEmpty()) {
            // create the condition
            conditions = "_user_ = '" + user + "'";
        } else {
            // add it to the existing conditions
            conditions = "_user_ = '" + user + "' AND (" + conditions + ")";
        }
        buf.append(" WHERE ").append(conditions);
        
        if (null != grouping) {
            buf.append(" GROUP BY ").append(grouping);
        }
        
        if (null != order) {
            buf.append(" ORDER BY ").append(order);
        }
        
        if (log.isTraceEnabled()) {
            log.trace("sqlQuery: " + buf);
        }
        
        if (!isSqlSafe(buf.toString())) {
            throw new IllegalArgumentException("Illegal arguments found");
        }
        
        return buf.toString();
    }
    
    public static String[] tokenizeOutsideParens(String fields, char c) {
        return tokenizeOutside(fields, new char[] {'(', ')'}, c);
    }
    
    public static String[] tokenizeOutside(String fields, char[] delimiters, char c) {
        if (delimiters == null || delimiters.length != 2) {
            return new String[] {fields};
        }
        char leftDelimiter = delimiters[0];
        char rightDelimiter = delimiters[1];
        List<String> result = new ArrayList<>();
        int start = 0;
        boolean inParens = false;
        for (int current = 0; current < fields.length(); current++) {
            char charAtCurrent = fields.charAt(current);
            if (charAtCurrent == leftDelimiter) {
                inParens = true;
            }
            if (charAtCurrent == rightDelimiter) {
                inParens = false;
            }
            boolean atLastChar = (current == fields.length() - 1);
            String trimmedSubstr = "";
            if (atLastChar) {
                trimmedSubstr = fields.substring(start).trim();
            } else if (charAtCurrent == c && !inParens) {
                trimmedSubstr = fields.substring(start, current).trim();
                start = current + 1;
            }
            if (!trimmedSubstr.isEmpty()) {
                result.add(trimmedSubstr);
            }
        }
        return result.toArray(new String[0]);
    }
    
    public String buildOrderClause(String order, Set<String> viewColumnNames) {
        if (order != null) {
            String[] commaSplit = tokenizeOutsideParens(order, ',');
            StringBuilder out = new StringBuilder();
            for (String s : commaSplit) {
                // in case its a function with a direction, separate them
                String[] spaceParsed = tokenizeOutsideParens(s, ' ');
                if (out.length() > 0) {
                    out.append(",");
                }
                for (int j = 0; j < spaceParsed.length; j++) {
                    // get rid of incoming back-tics
                    spaceParsed[j] = spaceParsed[j].replace("`", "").trim();
                    spaceParsed[j] = quoteField(spaceParsed[j], viewColumnNames);
                    if (j > 0) {
                        out.append(" ");
                    }
                    out.append(spaceParsed[j]);
                }
            }
            if (out.length() == 0) {
                order = null;
            } else {
                order = out.toString();
            }
        }
        return order;
    }
    
    private boolean isSqlSafe(String sqlQuery) {
        
        boolean isSqlSafe = true;
        String compareString = sqlQuery.toUpperCase();
        
        for (String b : cachedResultsQueryProperties.getReservedStatements()) {
            if (compareString.matches(b)) {
                isSqlSafe = false;
                break;
            }
        }
        
        return isSqlSafe;
    }
    
    // Ensure that identifiers (column names, etc) are quoted with backticks.
    private String quoteField(String field, Set<String> viewColumnNames) {
        if (!field.equals("*") && field.contains(".")) {
            if (isFunction(field)) {
                if (!field.contains("(*)")) {
                    // Parse the arguments to the function
                    int startParen = field.lastIndexOf(LPAREN) + 1;
                    int endParen = field.indexOf(RPAREN);
                    String[] args = field.substring(startParen, endParen).split(",");
                    for (String arg : args) {
                        if (!arg.contains("'") && !arg.contains("\"")) {
                            if (viewColumnNames != null && viewColumnNames.contains(arg)) {
                                field = field.replaceAll(arg, BACKTICK + arg + BACKTICK);
                            }
                        }
                    }
                }
                return field;
            } else {
                return BACKTICK + field + BACKTICK;
            }
        } else {
            return field;
        }
    }
    
    private boolean isFunction(String field) {
        if (field.contains(LPAREN) && field.contains(RPAREN) && field.indexOf(LPAREN) > 0) {
            boolean matches = false;
            for (String pattern : cachedResultsQueryProperties.getAllowedFunctions()) {
                matches = field.matches(pattern);
                if (matches) {
                    break;
                }
            }
            if (!matches) {
                throw new IllegalArgumentException(
                                "Function not allowed. Allowed functions are: " + StringUtils.join(cachedResultsQueryProperties.getAllowedFunctions(), ","));
            }
            return true;
        }
        return false;
    }
    
    public BaseQueryResponse getRows(String key, Integer rowBegin, Integer rowEnd, ProxiedUserDetails currentUser) throws QueryException {
        try {
            String user = ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName());
            if (log.isDebugEnabled()) {
                log.info("Request: {}/getRows from {} with rowBegin: {} rowEnd: {}", key, user, rowBegin, rowEnd);
            } else {
                log.info("Request: {}/getRows from {}", key, user);
            }
            
            // make sure the query is valid, and the user can act on it
            CachedResultsQueryStatus cachedResultsQueryStatus = validateRequest(key, currentUser);
            
            if (cachedResultsQueryStatus.getState() == CREATED) {
                return getRows(cachedResultsQueryStatus, rowBegin, rowEnd);
            } else {
                throw new BadRequestQueryException("Cannot call getRows on a query that has not finished creating", HttpStatus.SC_BAD_REQUEST + "-1");
            }
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            QueryException queryException = new QueryException(DatawaveErrorCode.QUERY_CLOSE_ERROR, e, "Unknown error getting rows for query " + key);
            log.error("Unknown error getting rows for query {}", key, queryException);
            throw queryException;
        }
    }
    
    private BaseQueryResponse getRows(CachedResultsQueryStatus cachedResultsQueryStatus, Integer rowBegin, Integer rowEnd)
                    throws QueryException, CloneNotSupportedException {
        
        if (rowBegin < 1) {
            throw new BadRequestQueryException(DatawaveErrorCode.ROW_BEGIN_LESS_THAN_1);
        }
        
        if (rowEnd != null && rowEnd < rowBegin) {
            throw new BadRequestQueryException(DatawaveErrorCode.ROW_END_LESS_THAN_ROW_BEGIN);
        }
        
        // If there is a this.maxPageSize set, then we should honor it here. Otherwise, we use Integer.MAX_VALUE
        int maxPageSize = cachedResultsQueryProperties.getMaxPageSize();
        if (rowEnd == null) {
            if (maxPageSize > 0) {
                rowEnd = (rowBegin + maxPageSize) - 1;
            } else {
                rowEnd = Integer.MAX_VALUE;
            }
        }
        
        int pagesize = (rowEnd - rowBegin) + 1;
        if (maxPageSize > 0 && pagesize > maxPageSize) {
            throw new QueryException(DatawaveErrorCode.TOO_MANY_ROWS_REQUESTED, MessageFormat.format("Size must be less than or equal to: {0}", maxPageSize));
        }
        
        // fetch the rows from sql
        final AtomicBoolean hitPageByteTrigger = new AtomicBoolean(false);
        final List<CacheableQueryRow> cacheableQueryRows = cachedResultsJdbcTemplate
                        .query(getSqlQuery(cachedResultsQueryStatus.getSqlQuery(), rowBegin, rowEnd), resultSet -> {
                            List<CacheableQueryRow> rows = new ArrayList<>();
                            
                            long resultBytes = 0;
                            while (resultSet.next() && !hitPageByteTrigger.get()) {
                                CacheableQueryRow row = CacheableQueryRowReader.createRow(resultSet, cachedResultsQueryStatus.getFixedFields(),
                                                responseObjectFactory, markingFunctions);
                                rows.add(row);
                                if (cachedResultsQueryProperties.getPageByteTrigger() != 0) {
                                    resultBytes += ObjectSizeOf.Sizer.getObjectSize(row);
                                    if (resultBytes >= cachedResultsQueryProperties.getPageByteTrigger()) {
                                        hitPageByteTrigger.set(true);
                                    }
                                }
                            }
                            
                            return rows;
                        });
        
        QueryLogic<?> queryLogic = queryLogicFactory.getQueryLogic(cachedResultsQueryStatus.getQueryLogicName(), cachedResultsQueryStatus.getCurrentUser());
        CacheableLogic cacheableLogic = (CacheableLogic) queryLogic.getTransformer(cachedResultsQueryStatus.getQuery());
        List<Object> results = new ArrayList<>();
        for (CacheableQueryRow cacheableQueryRow : cacheableQueryRows) {
            results.add(cacheableLogic.readFromCache(cacheableQueryRow));
        }
        
        BaseQueryResponse response;
        if (!results.isEmpty()) {
            ResultsPage<?> resultsPage = new ResultsPage<>(results, (hitPageByteTrigger.get() ? ResultsPage.Status.PARTIAL : ResultsPage.Status.COMPLETE));
            
            response = queryLogic.getEnrichedTransformer(cachedResultsQueryStatus.getQuery()).createResponse(resultsPage);
            
            response.setHasResults(true);
            response.setLogicName(cachedResultsQueryStatus.getQueryLogicName());
            response.setQueryId(cachedResultsQueryStatus.getAlias());
            if (response instanceof TotalResultsAware) {
                ((TotalResultsAware) response).setTotalResults(cachedResultsQueryStatus.getRowsWritten());
            }
        } else {
            throw new NoResultsQueryException(DatawaveErrorCode.NO_CONTENT_STATUS);
        }
        
        return response;
    }
    
    private String getSqlQuery(String sqlQuery, int beginRow, int endRow) {
        int limit = endRow - beginRow + 1;
        int offset = beginRow - 1;
        
        String limitTerm = " LIMIT " + limit;
        if (offset > 0) {
            limitTerm = " LIMIT " + offset + "," + limit;
        }
        
        return sqlQuery + limitTerm;
    }
    
    public GenericResponse<String> status(String key, ProxiedUserDetails currentUser) throws QueryException {
        log.info("Request: {}/status from {}", key, ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName()));
        
        CachedResultsQueryStatus cachedResultsQueryStatus = validateRequest(key, currentUser);
        
        GenericResponse<String> response = new GenericResponse<>();
        response.setResult(cachedResultsQueryStatus.getState().name());
        return response;
    }
    
    public CachedResultsDescribeResponse describe(String key, ProxiedUserDetails currentUser) throws QueryException {
        log.info("Request: {}/describe from {}", key, ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName()));
        
        CachedResultsQueryStatus cachedResultsQueryStatus = validateRequest(key, currentUser);
        
        CachedResultsDescribeResponse response = new CachedResultsDescribeResponse();
        response.setView(cachedResultsQueryStatus.getView());
        response.setColumns(new ArrayList<>(cachedResultsQueryStatus.getFieldIndexMap().keySet()));
        response.setNumRows(cachedResultsQueryStatus.getRowsWritten());
        return response;
    }
    
    public VoidResponse cancel(String key, ProxiedUserDetails currentUser) throws QueryException {
        log.info("Request: {}/cancel from {}", key, ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName()));
        return cancel(key, currentUser, false);
    }
    
    public VoidResponse adminCancel(String key, ProxiedUserDetails currentUser) throws QueryException {
        log.info("Request: {}/adminCancel from {}", key, ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName()));
        return cancel(key, currentUser, true);
    }
    
    private VoidResponse cancel(String key, ProxiedUserDetails currentUser, boolean adminOverride) throws QueryException {
        try {
            CachedResultsQueryStatus cachedResultsQueryStatus = validateRequest(key, currentUser, adminOverride);
            
            // cancel the running query if we are still loading
            if (cachedResultsQueryStatus.getState() == LOADING && cachedResultsQueryStatus.getRunningQueryId() != null) {
                queryService.cancel(cachedResultsQueryStatus.getRunningQueryId(), currentUser);
            }
            
            cachedResultsQueryStatus.setState(CANCELED);
            cachedResultsQueryCache.update(cachedResultsQueryStatus.getDefinedQueryId(), cachedResultsQueryStatus);
            
            return new VoidResponse();
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            QueryException queryException = new QueryException(DatawaveErrorCode.CANCELLATION_ERROR, e, "Unknown error canceling cached results query " + key);
            log.error("Unknown error canceling cached results query {}", key, queryException);
            throw queryException;
        }
    }
    
    public VoidResponse close(String key, ProxiedUserDetails currentUser) throws QueryException {
        log.info("Request: {}/close from {}", key, ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName()));
        return close(key, currentUser, false);
    }
    
    public VoidResponse adminClose(String key, ProxiedUserDetails currentUser) throws QueryException {
        log.info("Request: {}/adminClose from {}", key, ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName()));
        return close(key, currentUser, true);
    }
    
    private VoidResponse close(String key, ProxiedUserDetails currentUser, boolean adminOverride) throws QueryException {
        try {
            CachedResultsQueryStatus cachedResultsQueryStatus = validateRequest(key, currentUser, adminOverride);
            
            // cancel the running query if we are still loading
            if (cachedResultsQueryStatus.getState() == LOADING && cachedResultsQueryStatus.getRunningQueryId() != null) {
                queryService.cancel(cachedResultsQueryStatus.getRunningQueryId(), currentUser);
            }
            
            // remove the query from the cache
            cachedResultsQueryCache.removeQueryStatus(cachedResultsQueryStatus.getDefinedQueryId());
            
            if (cachedResultsQueryStatus.getCachedQueryId() != null) {
                cachedResultsQueryCache.removeQueryIdByCachedQueryIdLookup(cachedResultsQueryStatus.getCachedQueryId());
            }
            
            if (cachedResultsQueryStatus.getAlias() != null) {
                cachedResultsQueryCache.removeQueryIdByAliasLookup(cachedResultsQueryStatus.getAlias());
            }
            
            if (cachedResultsQueryStatus.getView() != null) {
                cachedResultsQueryCache.removeQueryIdByViewLookup(cachedResultsQueryStatus.getView());
            }
            
            return new VoidResponse();
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            QueryException queryException = new QueryException(DatawaveErrorCode.CLOSE_ERROR, e, "Unknown error closing cached results query " + key);
            log.error("Unknown error closing cached results query {}", key, queryException);
            throw queryException;
        }
    }
    
    public CachedResultsResponse setAlias(String key, String alias, ProxiedUserDetails currentUser) throws QueryException {
        try {
            log.info("Request: {}/setAlias from {} with alias {}", key, ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName()), alias);
            
            CachedResultsQueryStatus cachedResultsQueryStatus = validateRequest(key, currentUser);
            
            if (cachedResultsQueryStatus.getAlias() != null) {
                cachedResultsQueryCache.removeQueryIdByAliasLookup(cachedResultsQueryStatus.getAlias());
            }
            
            cachedResultsQueryStatus.setAlias(alias);
            cachedResultsQueryCache.update(cachedResultsQueryStatus.getDefinedQueryId(), cachedResultsQueryStatus);
            cachedResultsQueryCache.putQueryIdByAliasLookup(alias, cachedResultsQueryStatus.getDefinedQueryId());
            
            CachedResultsResponse response = new CachedResultsResponse();
            response.setOriginalQueryId(cachedResultsQueryStatus.getRunningQueryId());
            response.setQueryId(cachedResultsQueryStatus.getCachedQueryId());
            response.setViewName(cachedResultsQueryStatus.getView());
            response.setAlias(cachedResultsQueryStatus.getAlias());
            response.setTotalRows(cachedResultsQueryStatus.getRowsWritten());
            return response;
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            QueryException queryException = new QueryException(e, "Unknown error setting alias for cached results query " + key);
            log.error("Unknown error setting alias for cached results query {}", key, queryException);
            throw queryException;
        }
    }
    
    public CachedResultsResponse update(String key, String fields, String conditions, String grouping, String order, Integer pagesize,
                    ProxiedUserDetails currentUser) throws QueryException {
        try {
            String user = ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName());
            if (log.isDebugEnabled()) {
                log.info("Request: {}/udpate from {} with fields: {}, contitions: {}, groupind: {}, order: {}, pagesize: {}", key, user, fields, conditions,
                                grouping, order, pagesize);
            } else {
                log.info("Request: {}/update from {}", key, user);
            }
            
            // make sure the query is valid, and the user can act on it
            CachedResultsQueryStatus cachedResultsQueryStatus = validateRequest(key, currentUser);
            
            if (cachedResultsQueryStatus.getState() == CREATED) {
                return update(cachedResultsQueryStatus, key, fields, conditions, grouping, order, pagesize);
            } else {
                throw new BadRequestQueryException("Cannot call update on a query that has not been created", HttpStatus.SC_BAD_REQUEST + "-1");
            }
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            QueryException queryException = new QueryException(DatawaveErrorCode.QUERY_CLOSE_ERROR, e, "Unknown error updating cached results query " + key);
            log.error("Unknown error updating cached results query {}", key, queryException);
            throw queryException;
        }
    }
    
    private CachedResultsResponse update(CachedResultsQueryStatus cachedResultsQueryStatus, String key, String fields, String conditions, String grouping,
                    String order, Integer pagesize) throws QueryException, InterruptedException, CloneNotSupportedException {
        if (pagesize == null || pagesize <= 0) {
            pagesize = cachedResultsQueryProperties.getDefaultPageSize();
        }
        
        int maxPageSize = cachedResultsQueryProperties.getMaxPageSize();
        if (maxPageSize > 0 && pagesize > maxPageSize) {
            throw new PreConditionFailedQueryException(DatawaveErrorCode.REQUESTED_PAGE_SIZE_TOO_LARGE,
                            MessageFormat.format("{0} > {1}.", pagesize, cachedResultsQueryProperties.getMaxPageSize()));
        }
        
        cachedResultsQueryStatus.setPageSize(pagesize);
        
        if (fields != null || conditions != null || grouping != null || order != null) {
            boolean fieldChanged = false;
            
            if (!StringUtils.equals(cachedResultsQueryStatus.getFields(), fields) || cachedResultsQueryStatus.getFields() != null) {
                cachedResultsQueryStatus.setFields(fields);
                fieldChanged = true;
            }
            if (!StringUtils.equals(cachedResultsQueryStatus.getConditions(), conditions) || cachedResultsQueryStatus.getConditions() != null) {
                cachedResultsQueryStatus.setConditions(conditions);
                fieldChanged = true;
            }
            if (!StringUtils.equals(cachedResultsQueryStatus.getGrouping(), grouping) || cachedResultsQueryStatus.getGrouping() != null) {
                cachedResultsQueryStatus.setGrouping(grouping);
                fieldChanged = true;
            }
            if (!StringUtils.equals(cachedResultsQueryStatus.getOrder(), order) || cachedResultsQueryStatus.getOrder() != null) {
                cachedResultsQueryStatus.setOrder(order);
                fieldChanged = true;
            }
            
            if (fieldChanged) {
                cachedResultsQueryStatus.setSqlQuery(generateSqlQuery(cachedResultsQueryStatus));
                
                QueryLogic<?> queryLogic = queryLogicFactory.getQueryLogic(cachedResultsQueryStatus.getQueryLogicName(),
                                cachedResultsQueryStatus.getCurrentUser());
                
                // audit the query again
                // @formatter:off
                audit(cachedResultsQueryStatus.getRunningQueryId(),
                        queryLogic.getAuditType(),
                        queryLogic.getLogicName(),
                        cachedResultsQueryStatus.getOrigQuery(),
                        cachedResultsQueryStatus.getSqlQuery(),
                        createAuditParameters(cachedResultsQueryStatus),
                        cachedResultsQueryStatus.getCurrentUser());
                // @formatter:on
            }
        }
        
        cachedResultsQueryCache.update(cachedResultsQueryStatus.getDefinedQueryId(), cachedResultsQueryStatus);
        
        CachedResultsResponse response = new CachedResultsResponse();
        response.setOriginalQueryId(cachedResultsQueryStatus.getRunningQueryId());
        response.setQueryId(cachedResultsQueryStatus.getCachedQueryId());
        response.setViewName(cachedResultsQueryStatus.getView());
        response.setAlias(cachedResultsQueryStatus.getAlias());
        response.setTotalRows(cachedResultsQueryStatus.getRowsWritten());
        return response;
    }
    
    private CachedResultsQueryStatus validateRequest(String key, ProxiedUserDetails currentUser) throws NotFoundQueryException, UnauthorizedQueryException {
        return validateRequest(key, currentUser, false);
    }
    
    private CachedResultsQueryStatus validateRequest(String key, ProxiedUserDetails currentUser, boolean adminOverride)
                    throws NotFoundQueryException, UnauthorizedQueryException {
        // does the query exist?
        CachedResultsQueryStatus cachedResultsQueryStatus = cachedResultsQueryCache.lookupQueryStatus(key);
        if (cachedResultsQueryStatus == null) {
            throw new NotFoundQueryException(DatawaveErrorCode.NO_QUERY_OBJECT_MATCH, MessageFormat.format("{0}", key));
        }
        
        // admin requests can operate on any query, regardless of ownership
        if (!adminOverride) {
            // does the current user own this query?
            String currentUserId = ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getDn().subjectDN());
            String ownerUserId = ProxiedEntityUtils.getShortName(cachedResultsQueryStatus.getCurrentUser().getPrimaryUser().getDn().subjectDN());
            if (!ownerUserId.equals(currentUserId)) {
                throw new UnauthorizedQueryException(DatawaveErrorCode.QUERY_OWNER_MISMATCH, MessageFormat.format("{0} != {1}", currentUserId, ownerUserId));
            }
        }
        
        return cachedResultsQueryStatus;
    }
    
    public ThreadLocal<SecurityMarking> getSecurityMarkingOverride() {
        return scopedSecurityMarking.getThreadLocalOverride();
    }
    
    public ThreadLocal<CachedResultsQueryParameters> getCachedResultsQueryParametersOverride() {
        return scopedCachedResultsQueryParameters.getThreadLocalOverride();
    }
}
