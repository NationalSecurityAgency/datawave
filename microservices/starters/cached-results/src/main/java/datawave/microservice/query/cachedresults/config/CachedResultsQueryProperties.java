package datawave.microservice.query.cachedresults.config;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "datawave.query.cached-results")
public class CachedResultsQueryProperties {
    
    private int numFields = 900;
    private int defaultPageSize = 20;
    private int maxPageSize = 0;
    private long pageByteTrigger = 0;
    private int maxInsertAttempts = 10;
    private int maxValueLength = 1073741824;
    private int daysToLive = 1;
    
    private long lockWaitTime = TimeUnit.SECONDS.toMillis(5);
    private TimeUnit lockWaitTimeUnit = TimeUnit.MILLISECONDS;
    // The amount of time that the lock will be held before being automatically released
    private long lockLeaseTime = TimeUnit.SECONDS.toMillis(30);
    private TimeUnit lockLeaseTimeUnit = TimeUnit.MILLISECONDS;
    private List<String> reservedStatements = Arrays.asList(".*[^\\\\];.*", ".*CREATE[\\s]+TABLE.*", ".*DROP[\\s]+TABLE.*", ".*ALTER[\\s]+TABLE.*",
                    ".*DROP[\\s]+DATABASE.*", ".*CREATE[\\s]+PROCEDURE.*", ".*DELETE[\\s].*", ".*INSERT[\\s].*");
    private List<String> allowedFunctions = Arrays.asList(".*COUNT\\(.*\\).*", ".*SUM\\(.*\\).*", ".*MIN\\(.*\\).*", ".*MAX\\(.*\\).*", ".*LOWER\\(.*\\).*",
                    ".*UPPER\\(.*\\).*", ".*INET_ATON\\(.*\\).*", ".*INET_NTOA\\(.*\\).*", ".*CONVERT\\(.*\\).*", ".*STR_TO_DATE\\(.*\\).*");
    private RemoteQuery remoteQuery = new RemoteQuery();
    private Statements statementTemplates = new Statements();
    
    public int getNumFields() {
        return numFields;
    }
    
    public void setNumFields(int numFields) {
        this.numFields = numFields;
    }
    
    public int getDefaultPageSize() {
        return defaultPageSize;
    }
    
    public void setDefaultPageSize(int defaultPageSize) {
        this.defaultPageSize = defaultPageSize;
    }
    
    public int getMaxPageSize() {
        return maxPageSize;
    }
    
    public void setMaxPageSize(int maxPageSize) {
        this.maxPageSize = maxPageSize;
    }
    
    public long getPageByteTrigger() {
        return pageByteTrigger;
    }
    
    public void setPageByteTrigger(long pageByteTrigger) {
        this.pageByteTrigger = pageByteTrigger;
    }
    
    public int getMaxInsertAttempts() {
        return maxInsertAttempts;
    }
    
    public void setMaxInsertAttempts(int maxInsertAttempts) {
        this.maxInsertAttempts = maxInsertAttempts;
    }
    
    public int getMaxValueLength() {
        return maxValueLength;
    }
    
    public void setMaxValueLength(int maxValueLength) {
        this.maxValueLength = maxValueLength;
    }
    
    public int getDaysToLive() {
        return daysToLive;
    }
    
    public void setDaysToLive(int daysToLive) {
        this.daysToLive = daysToLive;
    }
    
    public long getLockWaitTime() {
        return lockWaitTime;
    }
    
    public long getLockWaitTimeMillis() {
        return lockWaitTimeUnit.toMillis(lockWaitTime);
    }
    
    public void setLockWaitTime(long lockWaitTime) {
        this.lockWaitTime = lockWaitTime;
    }
    
    public TimeUnit getLockWaitTimeUnit() {
        return lockWaitTimeUnit;
    }
    
    public long getLockLeaseTimeMillis() {
        return lockLeaseTimeUnit.toMillis(lockLeaseTime);
    }
    
    public void setLockWaitTimeUnit(TimeUnit lockWaitTimeUnit) {
        this.lockWaitTimeUnit = lockWaitTimeUnit;
    }
    
    public long getLockLeaseTime() {
        return lockLeaseTime;
    }
    
    public void setLockLeaseTime(long lockLeaseTime) {
        this.lockLeaseTime = lockLeaseTime;
    }
    
    public TimeUnit getLockLeaseTimeUnit() {
        return lockLeaseTimeUnit;
    }
    
    public void setLockLeaseTimeUnit(TimeUnit lockLeaseTimeUnit) {
        this.lockLeaseTimeUnit = lockLeaseTimeUnit;
    }
    
    public List<String> getReservedStatements() {
        return reservedStatements;
    }
    
    public void setReservedStatements(List<String> reservedStatements) {
        this.reservedStatements = reservedStatements;
    }
    
    public List<String> getAllowedFunctions() {
        return allowedFunctions;
    }
    
    public void setAllowedFunctions(List<String> allowedFunctions) {
        this.allowedFunctions = allowedFunctions;
    }
    
    public RemoteQuery getRemoteQuery() {
        return remoteQuery;
    }
    
    public void setRemoteQuery(RemoteQuery remoteQuery) {
        this.remoteQuery = remoteQuery;
    }
    
    public Statements getStatementTemplates() {
        return statementTemplates;
    }
    
    public void setStatementTemplates(Statements statementTemplates) {
        this.statementTemplates = statementTemplates;
    }
    
    public static class RemoteQuery {
        private String queryServiceUri = "https://query:8443/query/v1/query";
        // max bytes to buffer for each rest call (-1 is unlimited)
        private int maxBytesToBuffer = -1;
        
        private long duplicateTimeout = 30;
        
        private TimeUnit duplicateTimeoutUnit = TimeUnit.SECONDS;
        
        private long nextTimeout = 1;
        
        private TimeUnit nextTimeoutUnit = TimeUnit.HOURS;
        
        private long closeTimeout = 1;
        
        private TimeUnit closeTimeoutUnit = TimeUnit.HOURS;
        
        private long cancelTimeout = 30;
        
        private TimeUnit cancelTimeoutUnit = TimeUnit.SECONDS;
        
        private long removeTimeout = 30;
        
        private TimeUnit remoteTimeoutUnit = TimeUnit.SECONDS;
        
        public String getQueryServiceUri() {
            return queryServiceUri;
        }
        
        public void setQueryServiceUri(String queryServiceUri) {
            this.queryServiceUri = queryServiceUri;
        }
        
        public int getMaxBytesToBuffer() {
            return maxBytesToBuffer;
        }
        
        public void setMaxBytesToBuffer(int maxBytesToBuffer) {
            this.maxBytesToBuffer = maxBytesToBuffer;
        }
        
        public long getDuplicateTimeout() {
            return duplicateTimeout;
        }
        
        public long getDuplicateTimeoutMillis() {
            return duplicateTimeoutUnit.toMillis(duplicateTimeout);
        }
        
        public void setDuplicateTimeout(long duplicateTimeout) {
            this.duplicateTimeout = duplicateTimeout;
        }
        
        public TimeUnit getDuplicateTimeoutUnit() {
            return duplicateTimeoutUnit;
        }
        
        public void setDuplicateTimeoutUnit(TimeUnit duplicateTimeoutUnit) {
            this.duplicateTimeoutUnit = duplicateTimeoutUnit;
        }
        
        public long getNextTimeout() {
            return nextTimeout;
        }
        
        public long getNextTimeoutMillis() {
            return nextTimeoutUnit.toMillis(nextTimeout);
        }
        
        public void setNextTimeout(long nextTimeout) {
            this.nextTimeout = nextTimeout;
        }
        
        public TimeUnit getNextTimeoutUnit() {
            return nextTimeoutUnit;
        }
        
        public void setNextTimeoutUnit(TimeUnit nextTimeoutUnit) {
            this.nextTimeoutUnit = nextTimeoutUnit;
        }
        
        public long getCloseTimeout() {
            return closeTimeout;
        }
        
        public long getCloseTimeoutMillis() {
            return closeTimeoutUnit.toMillis(closeTimeout);
        }
        
        public void setCloseTimeout(long closeTimeout) {
            this.closeTimeout = closeTimeout;
        }
        
        public TimeUnit getCloseTimeoutUnit() {
            return closeTimeoutUnit;
        }
        
        public void setCloseTimeoutUnit(TimeUnit closeTimeoutUnit) {
            this.closeTimeoutUnit = closeTimeoutUnit;
        }
        
        public long getCancelTimeout() {
            return cancelTimeout;
        }
        
        public long getCancelTimeoutMillis() {
            return cancelTimeoutUnit.toMillis(cancelTimeout);
        }
        
        public void setCancelTimeout(long cancelTimeout) {
            this.cancelTimeout = cancelTimeout;
        }
        
        public TimeUnit getCancelTimeoutUnit() {
            return cancelTimeoutUnit;
        }
        
        public void setCancelTimeoutUnit(TimeUnit cancelTimeoutUnit) {
            this.cancelTimeoutUnit = cancelTimeoutUnit;
        }
        
        public long getRemoveTimeout() {
            return removeTimeout;
        }
        
        public long getRemoveTimeoutMillis() {
            return remoteTimeoutUnit.toMillis(removeTimeout);
        }
        
        public void setRemoveTimeout(long removeTimeout) {
            this.removeTimeout = removeTimeout;
        }
        
        public TimeUnit getRemoteTimeoutUnit() {
            return remoteTimeoutUnit;
        }
        
        public void setRemoteTimeoutUnit(TimeUnit remoteTimeoutUnit) {
            this.remoteTimeoutUnit = remoteTimeoutUnit;
        }
    }
    
    public static class Statements {
        private String createTableTemplate;
        private String createTable;
        private String dropTable;
        private String dropView;
        private String insert;
        private String createView;
        private String listExpiredTablesAndViews;
        
        public String getCreateTableTemplate() {
            return createTableTemplate;
        }
        
        public void setCreateTableTemplate(String createTableTemplate) {
            this.createTableTemplate = createTableTemplate;
        }
        
        public String getCreateTable() {
            return createTable;
        }
        
        public void setCreateTable(String createTable) {
            this.createTable = createTable;
        }
        
        public String getDropTable() {
            return dropTable;
        }
        
        public void setDropTable(String dropTable) {
            this.dropTable = dropTable;
        }
        
        public String getDropView() {
            return dropView;
        }
        
        public void setDropView(String dropView) {
            this.dropView = dropView;
        }
        
        public String getInsert() {
            return insert;
        }
        
        public void setInsert(String insert) {
            this.insert = insert;
        }
        
        public String getCreateView() {
            return createView;
        }
        
        public void setCreateView(String createView) {
            this.createView = createView;
        }
        
        public String getListExpiredTablesAndViews() {
            return listExpiredTablesAndViews;
        }
        
        public void setListExpiredTablesAndViews(String listExpiredTablesAndViews) {
            this.listExpiredTablesAndViews = listExpiredTablesAndViews;
        }
    }
}
