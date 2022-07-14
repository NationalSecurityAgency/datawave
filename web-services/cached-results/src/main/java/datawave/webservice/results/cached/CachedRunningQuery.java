package datawave.webservice.results.cached;

import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.QueryMetricFactory;
import datawave.webservice.query.Query;
import datawave.webservice.query.cache.AbstractRunningQuery;
import datawave.webservice.query.cache.ResultsPage;
import datawave.webservice.query.cachedresults.CacheableLogic;
import datawave.webservice.query.cachedresults.CacheableQueryRow;
import datawave.webservice.query.cachedresults.CacheableQueryRowReader;
import datawave.webservice.query.data.ObjectSizeOf;
import datawave.webservice.query.logic.QueryLogic;
import datawave.webservice.query.logic.QueryLogicFactory;
import datawave.webservice.query.logic.QueryLogicTransformer;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import javax.sql.DataSource;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetProvider;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.security.Principal;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

@SuppressWarnings("restriction")
public class CachedRunningQuery extends AbstractRunningQuery {
    
    private static Logger log = Logger.getLogger(CachedRunningQuery.class);
    
    private static DataSource datasource = null;
    
    private static final long serialVersionUID = 1L;
    
    private static ResponseObjectFactory responseObjectFactory;
    private transient Connection connection = null;
    private transient CachedRowSet crs = null;
    private transient Statement statement = null;
    
    private transient CacheableLogic cacheableLogic = null;
    private transient QueryLogic<?> queryLogic = null;
    private transient QueryLogicTransformer transformer = null;
    
    // gets set in previous and next
    private transient int lastPageNumber = 0;
    private transient int totalRows = 0;
    
    private enum position {
        BEFORE_FIRST, MIDDLE, AFTER_LAST
    };
    
    private transient position currentRow = position.BEFORE_FIRST;
    private static QueryLogicFactory queryFactory = null;
    
    // fields below are persisted
    
    private TreeSet<String> variableFields = new TreeSet<>();
    private String alias = null;
    private String originalQueryId = null;
    private String queryId = null;
    private Integer pagesize = -1;
    private String user = null;
    private String userDN = null;
    private String sqlQuery = null;
    private Set<String> fixedFieldsInEvent = new HashSet<>();
    private List<String> viewColumnNames = null;
    
    // Query definition
    private Query query = null;
    private String queryLogicName = null;
    private String view = null;
    private String tableName = null;
    private String fields = null;
    private String conditions = null;
    private String grouping = null;
    private String order = null;
    private Status status = Status.NONE;
    private String statusMessage = "";
    private Principal principal = null;
    
    private boolean shouldAutoActivate = false;
    
    private static List<String> reserved = new ArrayList<>();
    private static Set<String> allowedFunctions = new HashSet<>();
    private static final String BACKTICK = "`";
    private static final String SPACE = " ";
    private static final String LPAREN = "(";
    private static final String RPAREN = ")";
    
    public enum Status {
        NONE, LOADING, LOADED, CREATING, CANCELED, ERROR, AVAILABLE
    };
    
    private static final String DEFAULT_ORDER_BY = " ORDER BY _eventId_";
    
    private static String createCrqTable = "CREATE TABLE IF NOT EXISTS cachedResultsQuery (" + "queryId VARCHAR(100) NOT NULL," + "alias VARCHAR(100),"
                    + "lastUpdate TIMESTAMP," + "pagesize LONG," + "user VARCHAR(50) NOT NULL," + "view VARCHAR(200)," + "tableName VARCHAR(200),"
                    + "status VARCHAR(20) NOT NULL," + "statusMessage VARCHAR(1000) NOT NULL," + "fields LONGTEXT," + "conditions LONGTEXT,"
                    + "grouping LONGTEXT," + "orderBy LONGTEXT," + "variableFields LONGTEXT," + "originalQuery LONGTEXT," + "originalQueryBegin TIMESTAMP,"
                    + "originalQueryEnd TIMESTAMP," + "originalQueryAuths LONGTEXT," + "originalQueryLogicName VARCHAR(100),"
                    + "originalQueryName VARCHAR(200)," + "originalQueryUserDn VARCHAR(200)," + "originalQueryId VARCHAR(200)," + "originalQueryPageSize LONG,"
                    + "fixedFieldsInEvent VARCHAR(2000)," + "optionalQueryParameters BLOB," + "UNIQUE (queryId))";
    
    private static String insertCrqTable = "INSERT INTO cachedResultsQuery ("
                    + "queryId, alias, lastUpdate, pagesize, user, view, tableName, status, statusMessage, fields, "
                    + "conditions, grouping, orderBy, variableFields, originalQuery, " + "originalQueryBegin, originalQueryEnd, originalQueryAuths, "
                    + "originalQueryLogicName, originalQueryName, "
                    + "originalQueryUserDn, originalQueryId, originalQueryPageSize, fixedFieldsInEvent, optionalQueryParameters) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    
    private static String updateCrqTable = "UPDATE cachedResultsQuery SET "
                    + "queryId=?, alias=?, lastUpdate=?, pagesize=?, user=?, view=?, tableName=?, status=?, statusMessage=?, fields=?, conditions=?, "
                    + "grouping=?, orderBy=?, variableFields=?, originalQuery=?, originalQueryBegin=?, originalQueryEnd=?, "
                    + "originalQueryAuths=?, originalQueryLogicName=?, originalQueryName=?, "
                    + "originalQueryUserDn=?, originalQueryId=?, originalQueryPageSize=?, fixedFieldsInEvent=?, optionalQueryParameters=? WHERE queryId=?";
    
    private static String updateSatusCrqTable = "INSERT INTO cachedResultsQuery (queryId, alias, user, status, statusMessage)  " + "VALUES (?, ?, ?, ?, ?) "
                    + "ON DUPLICATE KEY UPDATE alias=?, lastUpdate=?, status=?, statusMessage=?";
    
    static {
        reserved.add(".*[^\\\\];.*");
        reserved.add(".*CREATE[\\s]+TABLE.*");
        reserved.add(".*DROP[\\s]+TABLE.*");
        reserved.add(".*ALTER[\\s]+TABLE.*");
        reserved.add(".*DROP[\\s]+DATABASE.*");
        reserved.add(".*CREATE[\\s]+PROCEDURE.*");
        reserved.add(".*DELETE[\\s].*");
        reserved.add(".*INSERT[\\s].*");
        
        allowedFunctions.add(".*COUNT\\(.*\\).*");
        allowedFunctions.add(".*SUM\\(.*\\).*");
        allowedFunctions.add(".*MIN\\(.*\\).*");
        allowedFunctions.add(".*MAX\\(.*\\).*");
        allowedFunctions.add(".*LOWER\\(.*\\).*");
        allowedFunctions.add(".*UPPER\\(.*\\).*");
        allowedFunctions.add(".*INET_ATON\\(.*\\).*");
        allowedFunctions.add(".*INET_NTOA\\(.*\\).*");
        allowedFunctions.add(".*CONVERT\\(.*\\).*");
        allowedFunctions.add(".*STR_TO_DATE\\(.*\\).*");
        
    }
    
    @Override
    public int hashCode() {
        int hashCode = 0;
        if (alias != null)
            hashCode += alias.hashCode();
        if (originalQueryId != null)
            hashCode += originalQueryId.hashCode();
        if (pagesize != null)
            hashCode += pagesize.hashCode();
        if (sqlQuery != null)
            hashCode += sqlQuery.hashCode();
        if (query != null)
            hashCode += query.hashCode();
        if (queryLogicName != null)
            hashCode += queryLogicName.hashCode();
        if (view != null)
            hashCode += view.hashCode();
        if (tableName != null)
            hashCode += tableName.hashCode();
        if (fields != null)
            hashCode += fields.hashCode();
        if (conditions != null)
            hashCode += conditions.hashCode();
        if (grouping != null)
            hashCode += grouping.hashCode();
        if (order != null)
            hashCode += order.hashCode();
        return hashCode;
    }
    
    private boolean fieldsEqual(Object thisField, Object otherField) {
        if (thisField == null && otherField != null) {
            return false;
        }
        if (thisField != null && otherField == null) {
            return false;
        }
        if (thisField == null && otherField == null) {
            return true;
        }
        return thisField.equals(otherField);
        
    }
    
    @Override
    public boolean equals(Object obj) {
        
        if (obj instanceof CachedRunningQuery) {
            CachedRunningQuery o = (CachedRunningQuery) obj;
            if (fieldsEqual(this.alias, o.alias) == false)
                return false;
            if (fieldsEqual(this.originalQueryId, o.originalQueryId) == false)
                return false;
            if (fieldsEqual(this.queryId, o.queryId) == false)
                return false;
            if (fieldsEqual(this.pagesize, o.pagesize) == false)
                return false;
            if (fieldsEqual(this.user, o.user) == false)
                return false;
            if (fieldsEqual(this.userDN, o.userDN) == false)
                return false;
            if (fieldsEqual(this.sqlQuery, o.sqlQuery) == false)
                return false;
            if (fieldsEqual(this.query, o.query) == false)
                return false;
            if (fieldsEqual(this.queryLogicName, o.queryLogicName) == false)
                return false;
            if (fieldsEqual(this.view, o.view) == false)
                return false;
            if (fieldsEqual(this.tableName, o.tableName) == false)
                return false;
            if (fieldsEqual(this.fields, o.fields) == false)
                return false;
            if (fieldsEqual(this.conditions, o.conditions) == false)
                return false;
            if (fieldsEqual(this.grouping, o.grouping) == false)
                return false;
            if (fieldsEqual(this.order, o.order) == false)
                return false;
            return true;
        } else {
            return false;
        }
    }
    
    private CachedRunningQuery(QueryMetricFactory metricFactory) {
        super(metricFactory);
    }
    
    private void setMetricsInfo() {
        
        BaseQueryMetric m = this.getMetric();
        
        // set the metric information
        m.setQueryType(this.getClass());
        m.setQueryId(this.queryId);
        m.setUser(this.user);
        m.setUserDN(this.query.getUserDN());
        if (this.query != null) {
            m.setColumnVisibility(this.query.getColumnVisibility());
            m.setQueryAuthorizations(this.query.getQueryAuthorizations());
            m.setQueryLogic(this.query.getQueryLogicName());
            m.setBeginDate(this.query.getBeginDate());
            m.setEndDate(this.query.getEndDate());
        }
    }
    
    public CachedRunningQuery(Query query, QueryLogic<?> queryLogic, String queryId, String alias, String user, String view, int pagesize,
                    String originalQueryId, Set<String> variableFields, Set<String> fixedFieldsInEvent, QueryMetricFactory metricFactory) {
        super(metricFactory);
        
        this.variableFields.clear();
        if (variableFields != null) {
            this.variableFields.addAll(variableFields);
        }
        this.originalQueryId = originalQueryId;
        this.query = query;
        if (queryLogic != null) {
            this.queryLogic = queryLogic;
            this.queryLogicName = queryLogic.getLogicName();
        }
        this.view = view;
        this.user = user;
        this.alias = alias;
        this.queryId = queryId;
        if (null != fixedFieldsInEvent) {
            this.fixedFieldsInEvent = fixedFieldsInEvent;
        }
        this.pagesize = pagesize;
        // if the CRQ is created through this constructor and is retrieved from the cache (and not from MySql)
        // then the first call to next() or previous() will cause the CachedResultsBean to get a MySql connection
        // and activate the CRQ. After the first call to nex() or previous() shouldAutoActivate willbe false.
        this.shouldAutoActivate = true;
        setMetricsInfo();
    }
    
    public CachedRunningQuery(Connection connection, Query query, QueryLogic<?> queryLogic, String queryId, String alias, String user, String view,
                    String fields, String conditions, String grouping, String order, int pagesize, Set<String> variableFields, Set<String> fixedFieldsInEvent,
                    QueryMetricFactory metricFactory) throws SQLException {
        super(metricFactory);
        
        this.variableFields.clear();
        if (variableFields != null) {
            this.variableFields.addAll(variableFields);
        }
        this.query = query;
        if (queryLogic != null) {
            this.queryLogic = queryLogic;
            this.queryLogicName = queryLogic.getLogicName();
        }
        this.view = view;
        this.fields = fields;
        this.conditions = conditions;
        this.grouping = grouping;
        this.order = order;
        this.user = user;
        this.connection = connection;
        this.queryId = queryId;
        this.alias = alias;
        this.pagesize = pagesize;
        if (null != fixedFieldsInEvent) {
            this.fixedFieldsInEvent = fixedFieldsInEvent;
        }
        this.sqlQuery = generateSql(this.view, this.fields, this.conditions, this.grouping, this.order, this.user, this.connection);
        // if the CRQ is created through this constructor and is retrieved from the cache (and not from MySql)
        // then the first call to next() or previous() will cause the CachedResultsBean to get a MySql connection
        // and activate the CRQ. After the first call to nex() or previous() shouldAutoActivate willbe false.
        this.shouldAutoActivate = true;
        activate(connection, queryLogic);
    }
    
    private static boolean isSqlSafe(String sqlQuery) {
        
        boolean isSqlSafe = true;
        String compareString = sqlQuery.toUpperCase();
        
        for (String b : CachedRunningQuery.reserved) {
            if (compareString.matches(b)) {
                isSqlSafe = false;
                break;
            }
        }
        
        return isSqlSafe;
    }
    
    // Ensure that identifiers (column names, etc) are quoted with backticks.
    private String quoteField(String field) {
        if (!field.equals("*") && field.contains(".")) {
            if (isFunction(field)) {
                if (!field.contains("(*)")) {
                    // Parse the arguments to the function
                    int startParen = field.lastIndexOf(LPAREN) + 1;
                    int endParen = field.indexOf(RPAREN);
                    String[] args = field.substring(startParen, endParen).split(",");
                    for (String arg : args) {
                        if (!arg.contains("'") && !arg.contains("\"")) {
                            if (this.viewColumnNames != null && this.viewColumnNames.contains(arg)) {
                                field = field.replaceAll(arg, BACKTICK + arg + BACKTICK);
                            }
                        }
                    }
                    return field;
                } else
                    return field;
            } else {
                return new StringBuilder().append(BACKTICK).append(field).append(BACKTICK).toString();
            }
        } else {
            return field;
        }
    }
    
    private boolean isFunction(String field) {
        if (field.contains(LPAREN) && field.contains(RPAREN) && field.indexOf(LPAREN) > 0) {
            boolean matches = false;
            for (String pattern : allowedFunctions) {
                matches = field.matches(pattern);
                if (matches)
                    break;
            }
            if (!matches)
                throw new IllegalArgumentException("Function not allowed. Allowed functions are: " + StringUtils.join(allowedFunctions, ","));
            return true;
        }
        return false;
    }
    
    public String generateSql(String view, String fields, String conditions, String grouping, String order, String user, Connection connection)
                    throws SQLException {
        CachedResultsParameters.validate(view);
        StringBuilder buf = new StringBuilder();
        if (StringUtils.isEmpty(StringUtils.trimToNull(fields)))
            fields = "*";
        if (StringUtils.isEmpty(StringUtils.trimToNull(conditions)))
            conditions = null;
        if (StringUtils.isEmpty(StringUtils.trimToNull(order)))
            order = null;
        if (StringUtils.isEmpty(StringUtils.trimToNull(grouping)))
            grouping = null;
        
        if (null == this.viewColumnNames)
            this.viewColumnNames = this.getViewColumnNames(connection, view);
        
        if (!fields.equals("*")) {
            LinkedHashSet<String> fieldSet = new LinkedHashSet<>();
            String[] result = tokenizeOutsideParens(fields, ',');
            
            LinkedHashSet<String> requestedFieldSet = new LinkedHashSet<>();
            
            for (String s : result) {
                s = s.replace("`", "").trim();
                s = quoteField(s);
                requestedFieldSet.add(s);
            }
            
            if (requestedFieldSet.contains("*") == true) {
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
                if (variableFields.contains(field) || isFunction(field)) {
                    newConditions.append(quoteField(field)).append(SPACE);
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
        
        order = buildOrderClause(order);
        
        if (null != grouping) {
            // quote fields in the group by
            List<String> newGroup = new ArrayList<>();
            String[] groupSplit = grouping.split(",");
            for (String s : groupSplit) {
                s = s.replace("`", "").trim();
                // add quoted field
                newGroup.add(quoteField(s));
            }
            if (newGroup.isEmpty()) {
                grouping = null;
            } else {
                grouping = StringUtils.join(newGroup, ",");
            }
            
        }
        
        if (conditions == null || conditions.isEmpty()) {
            // create the condition
            conditions = "_user_ = '" + user + "'";
        } else {
            // add it to the existing conditions
            conditions = "_user_ = '" + user + "' AND (" + conditions + ")";
        }
        
        buf.append("SELECT ").append(fields).append(" FROM ").append(view);
        if (null != conditions)
            buf.append(" WHERE ").append(conditions);
        if (null != grouping)
            buf.append(" GROUP BY ").append(grouping);
        if (null != order)
            buf.append(" ORDER BY ").append(order);
        
        if (log.isTraceEnabled()) {
            log.trace("sqlQuery: " + buf);
        }
        
        if (CachedRunningQuery.isSqlSafe(buf.toString()) == false) {
            throw new IllegalArgumentException("Illegal arguments found");
        }
        return buf.toString();
    }
    
    public boolean update(String fields, String conditions, String grouping, String order, Integer pagesize) throws SQLException {
        try {
            boolean mustInitialize = false;
            boolean fieldChanged = false;
            
            if (!StringUtils.equals(this.fields, fields) || this.fields != null) {
                this.fields = fields;
                fieldChanged = true;
            }
            if (!StringUtils.equals(this.conditions, conditions) || this.conditions != null) {
                this.conditions = conditions;
                fieldChanged = true;
            }
            if (!StringUtils.equals(this.grouping, grouping) || this.grouping != null) {
                this.grouping = grouping;
                fieldChanged = true;
            }
            if (!StringUtils.equals(this.order, order) || this.order != null) {
                this.order = order;
                fieldChanged = true;
            }
            
            String newSql = null;
            if (this.view != null) {
                
                if (fieldChanged == true) {
                    newSql = generateSql(this.view, this.fields, this.conditions, this.grouping, this.order, this.user, this.connection);
                    // only if changed
                    if (!newSql.equals(this.sqlQuery)) {
                        mustInitialize = true;
                    }
                    
                    if (pagesize != null) {
                        if (!pagesize.equals(this.pagesize)) {
                            this.pagesize = pagesize;
                            mustInitialize = true;
                        }
                    }
                }
            }
            
            return mustInitialize;
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }
    
    public boolean isActivated() {
        if (this.connection != null && this.statement != null && this.crs != null) {
            return true;
        } else {
            return false;
        }
    }
    
    private List<String> getViewColumnNames(Connection connection, String view) throws SQLException {
        CachedResultsParameters.validate(view);
        List<String> columns = new ArrayList<>();
        try (Statement s = connection.createStatement(); ResultSet rs = s.executeQuery("show columns from " + view)) {
            Set<String> fixedColumns = CacheableQueryRow.getFixedColumnSet();
            
            while (rs.next()) {
                String column = rs.getString(1);
                if (fixedColumns.contains(column) == false) {
                    columns.add(column);
                }
            }
        }
        
        return columns;
    }
    
    public void activate(Connection connection, QueryLogic<?> queryLogic) throws SQLException {
        
        this.connection = connection;
        this.transformer = queryLogic.getTransformer(this.query);
        if (this.transformer instanceof CacheableLogic) {
            this.cacheableLogic = (CacheableLogic) this.transformer;
            this.queryLogic = queryLogic;
        } else {
            throw new IllegalArgumentException(queryLogic.getLogicName() + " does not support CachedResults calls");
        }
        
        long start = System.currentTimeMillis();
        
        try {
            
            if (log.isTraceEnabled()) {
                String host = System.getProperty("jboss.host.name");
                log.trace("activating CRS on host:" + host + ", " + this);
            }
            
            this.statement = this.connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            initialize();
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            throw e;
        } finally {
            // update timestamp in case this operation to a long time.
            updateTimestamp();
            setMetricsInfo();
            this.getMetric().setSetupTime((System.currentTimeMillis() - start));
        }
    }
    
    private void initialize() throws SQLException {
        
        this.sqlQuery = this.generateSql(this.view, this.fields, this.conditions, this.grouping, this.order, this.user, this.connection);
        this.getMetric().setQuery(sqlQuery);
        
        this.crs = RowSetProvider.newFactory().createCachedRowSet();
        this.crs.setCommand(this.sqlQuery);
        
        String countQuery = "SELECT count(*) FROM (" + this.sqlQuery + ") AS CNT";
        log.trace("Count query: " + countQuery);
        try (Statement s = connection.createStatement(); ResultSet rs = s.executeQuery(countQuery)) {
            if (rs.next())
                this.totalRows = rs.getInt(1);
            else
                throw new SQLException("Count query did not return a result");
        }
        
        if (this.pagesize < this.totalRows) {
            this.crs.setPageSize(this.pagesize);
        } else {
            this.crs.setPageSize(this.totalRows);
        }
        
        if (log.isTraceEnabled()) {
            log.trace("Setting pageSize to " + crs.getPageSize());
            log.trace("Setting maxRows to " + crs.getMaxRows());
            log.trace("Setting totalRows to " + this.totalRows);
        }
        
        this.crs.execute(this.connection);
        this.crs.beforeFirst();
        this.currentRow = position.BEFORE_FIRST;
    }
    
    public String getUser() {
        return this.user;
    }
    
    /**
     * Return a specific page of results.
     * 
     * @return next page of results
     * @throws SQLException
     */
    public ResultsPage getRows(Integer rowBegin, Integer rowEnd, long pageByteTrigger) throws SQLException {
        if (log.isTraceEnabled())
            log.trace("getRows: begin=" + rowBegin + ", end=" + rowEnd);
        // update timestamp in case this operation takes a long time.
        updateTimestamp();
        long pageStartTime = System.currentTimeMillis();
        
        // We need to apply a default ORDER BY clause if one does not EXIST in the query
        StringBuilder query = new StringBuilder(this.sqlQuery);
        if (!this.sqlQuery.toUpperCase().contains(" ORDER BY ")) {
            query.append(DEFAULT_ORDER_BY);
        }
        
        ResultsPage resultList;
        int pagesize = (rowEnd - rowBegin) + 1;
        
        try (PreparedStatement ps = connection.prepareStatement(query.toString()); CachedRowSet crs = RowSetProvider.newFactory().createCachedRowSet()) {
            log.debug("Get Rows query: " + query);
            
            ps.setFetchSize(pagesize);
            crs.setPageSize(pagesize);
            
            try (ResultSet rs = ps.executeQuery()) {
                crs.populate(rs, rowBegin);
                resultList = convert(crs, rowBegin, rowEnd, pageByteTrigger);
            }
            
            // Update the metric
            long now = System.currentTimeMillis();
            this.getMetric().addPageTime(resultList.getResults().size(), (now - pageStartTime), pageStartTime, now);
            updateTimestamp();
            return resultList;
        }
    }
    
    private boolean nextPageOfResults() {
        
        boolean hasRows = false;
        if (this.totalRows > 0) {
            if (currentRow == position.BEFORE_FIRST) {
                // if we are at position.BEFORE_FIRST and rows exist, the the crs will already contain the first page
                hasRows = true;
            } else {
                try {
                    if (crs.nextPage()) {
                        crs.last();
                        hasRows = crs.getRow() > 0;
                        crs.beforeFirst();
                    }
                } catch (SQLException e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
        return hasRows;
    }
    
    private boolean previousPageOfResults() {
        
        boolean hasRows = false;
        if (this.totalRows > 0) {
            try {
                if (crs.previousPage()) {
                    crs.last();
                    hasRows = crs.getRow() > 0;
                    crs.beforeFirst();
                }
            } catch (SQLException e) {
                log.error(e.getMessage(), e);
            }
        }
        return hasRows;
    }
    
    /**
     * Return the next page of results
     * 
     * @return next page of results
     * @throws SQLException
     */
    public ResultsPage next(long pageByteTrigger) throws SQLException {
        // update timestamp in case this operation takes a long time.
        updateTimestamp();
        // only auto-activate before the first call to next/previous
        this.shouldAutoActivate = false;
        long pageStartTime = System.currentTimeMillis();
        
        if (currentRow == position.BEFORE_FIRST) {
            this.lastPageNumber = 0;
        }
        
        ResultsPage resultList = new ResultsPage();
        if (nextPageOfResults()) {
            resultList = convert(this.crs, pageByteTrigger);
        }
        
        if (!resultList.getResults().isEmpty()) {
            currentRow = position.MIDDLE;
            this.lastPageNumber++;
        } else {
            currentRow = position.AFTER_LAST;
            this.lastPageNumber = ((int) Math.ceil((float) this.totalRows / (float) this.pagesize)) + 1;
        }
        
        // Update the metric
        long now = System.currentTimeMillis();
        this.getMetric().addPageTime(resultList.getResults().size(), (now - pageStartTime), pageStartTime, now);
        updateTimestamp();
        
        return resultList;
    }
    
    /**
     * Return the previous page of results
     * 
     * @return previous page of results
     * @throws SQLException
     */
    public ResultsPage previous(long pageByteTrigger) throws SQLException {
        // update timestamp in case this operation takes a long time.
        updateTimestamp();
        // only auto-activate before the first call to next/previous
        this.shouldAutoActivate = false;
        long pageStartTime = System.currentTimeMillis();
        
        if (currentRow == position.AFTER_LAST) {
            this.lastPageNumber = ((int) Math.ceil((float) this.totalRows / (float) this.pagesize)) + 1;
        }
        
        ResultsPage resultList = new ResultsPage();
        if (previousPageOfResults()) {
            resultList = convert(this.crs, pageByteTrigger);
        }
        
        if (!resultList.getResults().isEmpty()) {
            currentRow = position.MIDDLE;
            this.lastPageNumber--;
        } else {
            currentRow = position.BEFORE_FIRST;
            this.lastPageNumber = 0;
        }
        
        // Update the metric
        long now = System.currentTimeMillis();
        this.getMetric().addPageTime(resultList.getResults().size(), (now - pageStartTime), pageStartTime, now);
        updateTimestamp();
        
        return resultList;
    }
    
    @Override
    public long getLastPageNumber() {
        return this.lastPageNumber;
    }
    
    /**
     * Convert the cached row set into a result list.
     * 
     * @param cachedRowSet
     * @param pageByteTrigger
     * @return
     */
    private ResultsPage convert(CachedRowSet cachedRowSet, long pageByteTrigger) {
        boolean hitPageByteTrigger = false;
        List<CacheableQueryRow> cacheableQueryRowList = new ArrayList<>();
        try {
            cachedRowSet.beforeFirst();
            long resultBytes = 0;
            while (cachedRowSet.next() && !hitPageByteTrigger) {
                CacheableQueryRow row = CacheableQueryRowReader.createRow(cachedRowSet, this.fixedFieldsInEvent);
                cacheableQueryRowList.add(row);
                if (pageByteTrigger != 0) {
                    resultBytes += ObjectSizeOf.Sizer.getObjectSize(row);
                    if (resultBytes >= pageByteTrigger) {
                        hitPageByteTrigger = true;
                    }
                }
            }
        } catch (SQLException | RuntimeException e) {
            log.error(e.getMessage(), e);
        }
        
        if (this.cacheableLogic == null) {
            return new ResultsPage();
        } else {
            return new ResultsPage(this.cacheableLogic.readFromCache(cacheableQueryRowList), (hitPageByteTrigger ? ResultsPage.Status.PARTIAL
                            : ResultsPage.Status.COMPLETE));
        }
    }
    
    private ResultsPage convert(CachedRowSet cachedRowSet, Integer rowBegin, Integer rowEnd, long pageByteTrigger) {
        boolean hitPageByteTrigger = false;
        List<CacheableQueryRow> cacheableQueryRowList = new ArrayList<>();
        try {
            long resultBytes = 0;
            while (cachedRowSet.next() && cachedRowSet.getRow() <= rowEnd && !hitPageByteTrigger) {
                if (log.isTraceEnabled())
                    log.trace("CRS.position: " + cachedRowSet.getRow() + ", size: " + cachedRowSet.size());
                CacheableQueryRow row = CacheableQueryRowReader.createRow(cachedRowSet, this.fixedFieldsInEvent);
                cacheableQueryRowList.add(row);
                if (pageByteTrigger != 0) {
                    resultBytes += ObjectSizeOf.Sizer.getObjectSize(row);
                    if (resultBytes >= pageByteTrigger) {
                        hitPageByteTrigger = true;
                    }
                }
            }
            
        } catch (SQLException | RuntimeException e) {
            log.error(e.getMessage(), e);
        }
        
        if (this.cacheableLogic == null) {
            return new ResultsPage();
        } else {
            return new ResultsPage(this.cacheableLogic.readFromCache(cacheableQueryRowList), (hitPageByteTrigger ? ResultsPage.Status.PARTIAL
                            : ResultsPage.Status.COMPLETE));
        }
    }
    
    public void resetConnection() {
        this.connection = null;
        this.statement = null;
        this.crs = null;
    }
    
    public Connection getConnection() {
        return this.connection;
    }
    
    @Override
    public String toString() {
        
        String host = System.getProperty("jboss.host.name");
        
        return new StringBuilder().append("host:").append(host).append(", alias:").append(alias).append(", originalQueryId:").append(originalQueryId)
                        .append(", queryId:").append(queryId).append(", pagesize:").append(pagesize).append(", user:").append(user).append(", sqlQuery:")
                        .append(sqlQuery).append(", query:").append(query).append(", queryLogicName:").append(queryLogicName).append(", view:").append(view)
                        .append(", tableName:").append(tableName).append(", fields:").append(fields).append(", conditions:").append(conditions)
                        .append(", grouping:").append(grouping).append(", order:").append(order).append(", fixedFieldsInEvent: ")
                        .append(this.fixedFieldsInEvent).toString();
        
    }
    
    public QueryLogic<?> getQueryLogic() {
        return queryLogic;
    }
    
    public String getQueryId() {
        return queryId;
    }
    
    public int getTotalRows() {
        return totalRows;
    }
    
    public QueryLogicTransformer getTransformer() {
        return transformer;
    }
    
    public int getPagesize() {
        return pagesize;
    }
    
    public String getView() {
        return view;
    }
    
    public String getFields() {
        return fields;
    }
    
    public String getGrouping() {
        return grouping;
    }
    
    public String getOrder() {
        return order;
    }
    
    public String getOriginalQueryId() {
        return originalQueryId;
    }
    
    public String getQueryLogicName() {
        return queryLogicName;
    }
    
    public String getAlias() {
        return alias;
    }
    
    public void setConnection(Connection connection) {
        this.connection = connection;
    }
    
    public Query getQuery() {
        return query;
    }
    
    private void updateTimestamp() {
        touch();
    }
    
    public void setVariableFields(Set<String> variableFields) {
        this.variableFields.clear();
        this.variableFields.addAll(variableFields);
    }
    
    public Set<String> getVariableFields() {
        return variableFields;
    }
    
    public void setAlias(String alias) {
        this.alias = alias;
    }
    
    public static void saveToDatabaseByQueryId(String queryId, String alias, String user, Status status, String statusMessage) {
        
        verifyCrqTableExists();
        
        try (Connection localConnection = datasource.getConnection(); PreparedStatement ps = localConnection.prepareStatement(updateSatusCrqTable)) {
            
            int x = 1;
            ps.setString(x++, queryId);
            ps.setString(x++, alias);
            ps.setString(x++, user);
            ps.setString(x++, status.toString());
            ps.setString(x++, statusMessage);
            ps.setString(x++, alias);
            ps.setTimestamp(x++, new Timestamp(System.currentTimeMillis()));
            ps.setString(x++, status.toString());
            ps.setString(x++, statusMessage);
            ps.addBatch();
            ps.executeBatch();
            
        } catch (SQLException | RuntimeException e) {
            log.error(e.getMessage(), e);
        }
    }
    
    public void saveToDatabase(Principal principal, QueryMetricFactory metricFactory) {
        
        CachedRunningQuery crq = CachedRunningQuery.retrieveFromDatabase(this.queryId, principal, metricFactory);
        if (crq == null) {
            saveToDatabase(false);
        } else {
            saveToDatabase(true);
        }
    }
    
    private static void verifyCrqTableExists() {
        
        try (Connection localConnection = datasource.getConnection(); Statement s = localConnection.createStatement()) {
            s.execute(createCrqTable);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
    
    private void saveToDatabase(boolean update) {
        
        verifyCrqTableExists();
        
        try (Connection localConnection = datasource.getConnection();
                        PreparedStatement ps = localConnection.prepareStatement(update ? updateCrqTable : insertCrqTable)) {
            
            int x = 1;
            ps.setString(x++, queryId);
            ps.setString(x++, alias);
            ps.setTimestamp(x++, new Timestamp(System.currentTimeMillis()));
            ps.setInt(x++, pagesize);
            ps.setString(x++, user);
            ps.setString(x++, view);
            ps.setString(x++, tableName);
            ps.setString(x++, status.toString());
            ps.setString(x++, (statusMessage.length() <= 200) ? statusMessage : statusMessage.substring(0, 200));
            ps.setString(x++, fields);
            ps.setString(x++, conditions);
            ps.setString(x++, grouping);
            ps.setString(x++, order);
            ps.setString(x++, StringUtils.join(variableFields, " "));
            
            // parts of the query
            if (query == null) {
                ps.setString(x++, null);
                ps.setLong(x++, 0);
                ps.setLong(x++, 0);
                ps.setString(x++, null);
                ps.setString(x++, null);
                ps.setString(x++, null);
                ps.setString(x++, null);
                ps.setString(x++, null);
                ps.setInt(x++, 10);
            } else {
                ps.setString(x++, query.getQuery());
                if (query.getBeginDate() != null) {
                    ps.setTimestamp(x++, new Timestamp(query.getBeginDate().getTime()));
                } else {
                    x++;
                }
                if (query.getEndDate() != null) {
                    ps.setTimestamp(x++, new Timestamp(query.getEndDate().getTime()));
                } else {
                    x++;
                }
                ps.setString(x++, query.getQueryAuthorizations());
                ps.setString(x++, query.getQueryLogicName());
                ps.setString(x++, query.getQueryName());
                ps.setString(x++, query.getOwner());
                ps.setString(x++, query.getId().toString());
                ps.setInt(x++, query.getPagesize());
            }
            
            // Set the fixedFields
            if (this.fixedFieldsInEvent.isEmpty())
                ps.setNull(x++, Types.VARCHAR);
            else
                ps.setString(x++, StringUtils.join(this.fixedFieldsInEvent, ","));
            
            MultiValueMap<String,String> optionalQueryParameters = new LinkedMultiValueMap<>(query.getOptionalQueryParameters());
            if (optionalQueryParameters == null || optionalQueryParameters.isEmpty())
                ps.setNull(x++, Types.BLOB);
            else
                ps.setObject(x++, optionalQueryParameters);
            
            if (update == true) {
                ps.setString(x++, queryId);
            }
            
            ps.addBatch();
            ps.executeBatch();
            
        } catch (SQLException | RuntimeException e) {
            log.error(e.getMessage(), e);
        }
    }
    
    public static void removeFromDatabase(String id) {
        
        verifyCrqTableExists();
        
        try (Connection localConnection = datasource.getConnection();
                        PreparedStatement s = localConnection.prepareStatement("DELETE FROM cachedResultsQuery WHERE queryId = ?")) {
            
            s.setString(1, id);
            s.execute();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
    
    public static CachedRunningQuery retrieveFromDatabase(String id, Principal principal, QueryMetricFactory metricFactory) {
        
        verifyCrqTableExists();
        
        CachedRunningQuery crq = null;
        
        String sql = "SELECT * FROM cachedResultsQuery WHERE alias=? OR queryId=? OR view=?";
        
        try (Connection localConnection = datasource.getConnection(); PreparedStatement statement = localConnection.prepareStatement(sql)) {
            
            statement.setString(1, id);
            statement.setString(2, id);
            statement.setString(3, id);
            // String sql = "SELECT * FROM cachedResultsQuery WHERE alias='" + id + "' OR queryId='" + id + "' OR view='" + id + "'";
            
            try (ResultSet resultSet = statement.executeQuery()) {
                
                resultSet.last();
                int numRows = resultSet.getRow();
                
                if (numRows > 0) {
                    
                    resultSet.first();
                    crq = new CachedRunningQuery(metricFactory);
                    int x = 1;
                    crq.queryId = resultSet.getString(x++);
                    crq.alias = resultSet.getString(x++);
                    // last updated
                    resultSet.getTimestamp(x++);
                    crq.pagesize = resultSet.getInt(x++);
                    crq.user = resultSet.getString(x++);
                    crq.view = CachedResultsParameters.validate(resultSet.getString(x++), true);
                    crq.tableName = resultSet.getString(x++);
                    
                    crq.getMetric().setQueryType(CachedRunningQuery.class);
                    crq.getMetric().setQueryId(crq.queryId);
                    crq.getMetric().setUser(crq.user);
                    
                    String s = resultSet.getString(x++);
                    if (s != null) {
                        crq.status = Status.valueOf(s);
                    }
                    crq.statusMessage = resultSet.getString(x++);
                    crq.fields = resultSet.getString(x++);
                    crq.conditions = resultSet.getString(x++);
                    crq.grouping = resultSet.getString(x++);
                    crq.order = resultSet.getString(x++);
                    String varFields = resultSet.getString(x++);
                    if (varFields != null) {
                        crq.variableFields.addAll(Arrays.asList(varFields.split(" ")));
                    }
                    
                    Query query = crq.responseObjectFactory.getQueryImpl();
                    
                    query.setQuery(resultSet.getString(x++));
                    Timestamp bDate = resultSet.getTimestamp(x++);
                    if (bDate == null) {
                        query.setBeginDate(null);
                    } else {
                        query.setBeginDate(new Date(bDate.getTime()));
                    }
                    Timestamp eDate = resultSet.getTimestamp(x++);
                    if (bDate == null) {
                        query.setBeginDate(null);
                    } else {
                        query.setEndDate(new Date(eDate.getTime()));
                    }
                    query.setQueryAuthorizations(resultSet.getString(x++));
                    query.setQueryLogicName(resultSet.getString(x++));
                    query.setQueryName(resultSet.getString(x++));
                    query.setUserDN(resultSet.getString(x++));
                    String uuid = resultSet.getString(x++);
                    if (uuid != null) {
                        query.setId(UUID.fromString(uuid));
                    }
                    query.setPagesize(resultSet.getInt(x++));
                    String fixedFields = resultSet.getString(x++);
                    if (!StringUtils.isEmpty(fixedFields)) {
                        for (String field : fixedFields.split(","))
                            crq.fixedFieldsInEvent.add(field.trim());
                    }
                    
                    Blob optionalQueryParametersBlob = resultSet.getBlob(x++);
                    if (optionalQueryParametersBlob != null) {
                        
                        try {
                            InputStream istream = optionalQueryParametersBlob.getBinaryStream();
                            ObjectInputStream oistream = new ObjectInputStream(istream);
                            Object optionalQueryParametersObject = oistream.readObject();
                            if (optionalQueryParametersObject != null && optionalQueryParametersObject instanceof MultiValueMap) {
                                MultiValueMap<String,String> optionalQueryParameters = (MultiValueMap<String,String>) optionalQueryParametersObject;
                                query.setOptionalQueryParameters(optionalQueryParameters);
                            }
                        } catch (IOException e) {
                            log.error(e.getMessage(), e);
                        } catch (ClassNotFoundException e) {
                            log.error(e.getMessage(), e);
                        }
                    }
                    
                    crq.query = query;
                    crq.queryLogicName = query.getQueryLogicName();
                    crq.originalQueryId = uuid;
                    if (crq.view != null && crq.fields != null && crq.user != null) {
                        crq.sqlQuery = crq.generateSql(crq.view, crq.fields, crq.conditions, crq.grouping, crq.order, crq.user, localConnection);
                    }
                    if (crq.queryLogicName != null) {
                        try {
                            crq.queryLogic = queryFactory.getQueryLogic(crq.queryLogicName, principal);
                        } catch (IllegalArgumentException | CloneNotSupportedException e) {
                            log.error(e.getMessage(), e);
                        }
                    }
                }
            }
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }
        return crq;
    }
    
    public static void setDatasource(DataSource datasource) {
        CachedRunningQuery.datasource = datasource;
    }
    
    public Status getStatus() {
        return status;
    }
    
    public void setStatus(Status status) {
        this.status = status;
    }
    
    public String getStatusMessage() {
        return statusMessage;
    }
    
    public void setStatusMessage(String statusMessage) {
        this.statusMessage = statusMessage;
    }
    
    public static void setQueryFactory(QueryLogicFactory queryFactory) {
        CachedRunningQuery.queryFactory = queryFactory;
    }
    
    public void setView(String view) {
        this.view = CachedResultsParameters.validate(view);
    }
    
    public String getTableName() {
        return tableName;
    }
    
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    
    public void setOriginalQueryId(String originalQueryId) {
        this.originalQueryId = originalQueryId;
    }
    
    public CachedRowSet getCrs() {
        return crs;
    }
    
    public Statement getStatement() {
        return statement;
    }
    
    public void setQuery(Query query) {
        this.query = query;
    }
    
    public static String[] tokenizeOutsideParens(String fields, char c) {
        return tokenizeOutside(fields, new char[] {'(', ')'}, c);
    }
    
    public static String[] tokenizeOutside(String fields, char[] delimiters, char c) {
        if (delimiters == null || delimiters.length != 2)
            return new String[] {fields};
        char leftDelimiter = delimiters[0];
        char rightDelimiter = delimiters[1];
        List<String> result = new ArrayList<>();
        int start = 0;
        boolean inParens = false;
        for (int current = 0; current < fields.length(); current++) {
            char charAtCurrent = fields.charAt(current);
            if (charAtCurrent == leftDelimiter)
                inParens = true;
            if (charAtCurrent == rightDelimiter)
                inParens = false;
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
        return result.toArray(new String[result.size()]);
    }
    
    public String buildOrderClause(String order) {
        if (order != null) {
            String[] commaSplit = tokenizeOutsideParens(order, ',');
            StringBuilder out = new StringBuilder();
            for (String s : commaSplit) {
                // in case its a function with a direction, separate them
                String[] spaceParsed = CachedRunningQuery.tokenizeOutsideParens(s, ' ');
                if (out.length() > 0)
                    out.append(",");
                for (int j = 0; j < spaceParsed.length; j++) {
                    // get rid of incoming back-tics
                    spaceParsed[j] = spaceParsed[j].replace("`", "").trim();
                    spaceParsed[j] = quoteField(spaceParsed[j]);
                    if (j > 0)
                        out.append(" ");
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
    
    public Principal getPrincipal() {
        return principal;
    }
    
    public void setPrincipal(Principal principal) {
        this.principal = principal;
    }
    
    public static void setResponseObjectFactory(ResponseObjectFactory responseObjectFactory) {
        CachedRunningQuery.responseObjectFactory = responseObjectFactory;
    }
    
    public boolean getShouldAutoActivate() {
        return this.shouldAutoActivate;
    }
    
    public void closeConnection(Logger log) {
        
        if (log.isTraceEnabled()) {
            log.trace("closing connections for query " + getQueryId());
        }
        
        Connection connection = getConnection();
        Statement statement = getStatement();
        CachedRowSet crs = getCrs();
        resetConnection();
        DbUtils.closeQuietly(connection, statement, crs);
    }
    
}
