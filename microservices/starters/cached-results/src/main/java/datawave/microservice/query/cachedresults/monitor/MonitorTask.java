package datawave.microservice.query.cachedresults.monitor;

import static datawave.microservice.query.cachedresults.CachedResultsQueryService.TABLE_PLACEHOLDER;
import static datawave.microservice.query.cachedresults.status.CachedResultsQueryStatus.CACHED_RESULTS_STATE.LOADING;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import datawave.microservice.query.cachedresults.CachedResultsQueryService;
import datawave.microservice.query.cachedresults.config.CachedResultsQueryProperties;
import datawave.microservice.query.cachedresults.monitor.cache.MonitorStatus;
import datawave.microservice.query.cachedresults.monitor.cache.MonitorStatusCache;
import datawave.microservice.query.cachedresults.monitor.config.MonitorProperties;
import datawave.microservice.query.cachedresults.status.CachedResultsQueryStatus;
import datawave.microservice.query.cachedresults.status.cache.CachedResultsQueryCache;
import datawave.webservice.query.exception.QueryException;

public class MonitorTask implements Callable<Void> {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private static final String DATABASE_NAME_PLACEHOLDER = "%DATABASE_NAME%";
    private static final String DAYS_TO_LIVE_PLACEHOLDER = "%DAYS_TO_LIVE%";
    
    private final MonitorProperties monitorProperties;
    private final CachedResultsQueryProperties cachedResultsQueryProperties;
    private final MonitorStatusCache monitorStatusCache;
    private final CachedResultsQueryService cachedResultsQueryService;
    private final CachedResultsQueryCache cachedResultsQueryCache;
    private final JdbcTemplate cachedResultsJdbcTemplate;
    private final String schema;
    
    public MonitorTask(MonitorProperties monitorProperties, CachedResultsQueryProperties cachedResultsQueryProperties, MonitorStatusCache monitorStatusCache,
                    CachedResultsQueryService cachedResultsQueryService, CachedResultsQueryCache cachedResultsQueryCache,
                    JdbcTemplate cachedResultsJdbcTemplate) throws SQLException {
        this.monitorProperties = monitorProperties;
        this.cachedResultsQueryProperties = cachedResultsQueryProperties;
        this.monitorStatusCache = monitorStatusCache;
        this.cachedResultsQueryService = cachedResultsQueryService;
        this.cachedResultsQueryCache = cachedResultsQueryCache;
        this.cachedResultsJdbcTemplate = cachedResultsJdbcTemplate;
        this.schema = cachedResultsJdbcTemplate.getDataSource().getConnection().getSchema();
    }
    
    @Override
    public Void call() throws Exception {
        if (tryLock()) {
            boolean success = false;
            MonitorStatus monitorStatus = null;
            try {
                long currentTimeMillis = System.currentTimeMillis();
                monitorStatus = monitorStatusCache.getStatus();
                if (monitorStatus.isExpired(currentTimeMillis, monitorProperties.getMonitorIntervalMillis())) {
                    monitor(currentTimeMillis);
                    success = true;
                }
            } finally {
                if (success) {
                    monitorStatus.setLastChecked(System.currentTimeMillis());
                    monitorStatusCache.setStatus(monitorStatus);
                }
                unlock();
            }
        }
        return null;
    }
    
    // Perform the following actions:
    // Find all result tables older than 24 hours
    // Drop the view & table for each of the result tables
    // Clean up the cache entries for the dropped tables/views
    private void monitor(long currentTimeMillis) {
        // lookup all of the result tables older than 24 hours
        List<String> expiredTables = cachedResultsJdbcTemplate.query(listExpiredTablesAndViews(), resultSet -> {
            List<String> tables = new ArrayList<>();
            while (resultSet.next()) {
                tables.add(resultSet.getString(1));
            }
            return tables;
        });
        
        // drop each of the expired views/tables
        if (expiredTables != null) {
            for (String tableName : expiredTables) {
                try {
                    String statement = null;
                    if (tableName.startsWith("t")) {
                        statement = cachedResultsQueryProperties.getStatementTemplates().getDropTable().replace(TABLE_PLACEHOLDER, tableName);
                    } else if (tableName.startsWith("v")) {
                        statement = cachedResultsQueryProperties.getStatementTemplates().getDropView().replace(TABLE_PLACEHOLDER, tableName);
                    }
                    
                    if (statement != null) {
                        cachedResultsJdbcTemplate.execute(statement);
                    }
                } catch (DataAccessException e) {
                    log.warn("Unable to drop expired table: {}", tableName, e);
                }
            }
            
            // delete the cache entries for the dropped tables/views
            for (String tableName : expiredTables) {
                CachedResultsQueryStatus cachedResultsQueryStatus = cachedResultsQueryCache.lookupQueryStatus(tableName);
                if (cachedResultsQueryStatus.getState() == LOADING) {
                    try {
                        cachedResultsQueryService.cancel(cachedResultsQueryStatus.getDefinedQueryId(), cachedResultsQueryStatus.getCurrentUser());
                    } catch (QueryException e) {
                        log.warn("Unable to cancel cached results query: {}", cachedResultsQueryStatus.getDefinedQueryId(), e);
                    }
                }
                
                if (cachedResultsQueryStatus.getAlias() != null) {
                    cachedResultsQueryCache.removeQueryIdByAliasLookup(cachedResultsQueryStatus.getAlias());
                }
                
                if (cachedResultsQueryStatus.getView() != null) {
                    cachedResultsQueryCache.removeQueryIdByViewLookup(cachedResultsQueryStatus.getView());
                }
                
                cachedResultsQueryCache.removeQueryStatus(cachedResultsQueryStatus.getDefinedQueryId());
            }
        }
    }
    
    private String listExpiredTablesAndViews() {
        return cachedResultsQueryProperties.getStatementTemplates().getListExpiredTablesAndViews().replace(DATABASE_NAME_PLACEHOLDER, schema)
                        .replace(DAYS_TO_LIVE_PLACEHOLDER, Integer.toString(cachedResultsQueryProperties.getDaysToLive()));
    }
    
    private boolean tryLock() throws InterruptedException {
        return monitorStatusCache.tryLock(monitorProperties.getLockWaitTime(), monitorProperties.getLockWaitTimeUnit(), monitorProperties.getLockLeaseTime(),
                        monitorProperties.getLockLeaseTimeUnit());
    }
    
    private void unlock() {
        monitorStatusCache.unlock();
    }
}
