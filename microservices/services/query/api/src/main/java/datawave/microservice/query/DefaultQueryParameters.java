package datawave.microservice.query;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;

public class DefaultQueryParameters implements QueryParameters {
    
    private static final List<String> KNOWN_PARAMS = Arrays.asList(QUERY_STRING, QUERY_NAME, QUERY_PERSISTENCE, QUERY_PAGESIZE, QUERY_PAGETIMEOUT,
                    QUERY_AUTHORIZATIONS, QUERY_EXPIRATION, QUERY_TRACE, QUERY_BEGIN, QUERY_END, QUERY_VISIBILITY, QUERY_LOGIC_NAME, QUERY_POOL,
                    QUERY_MAX_RESULTS_OVERRIDE, QUERY_MAX_CONCURRENT_TASKS, QUERY_SYSTEM_FROM);
    
    protected String query;
    protected String queryName;
    protected QueryPersistence persistenceMode;
    protected int pagesize;
    protected int pageTimeout;
    protected boolean isMaxResultsOverridden;
    protected long maxResultsOverride;
    protected String auths;
    protected Date expirationDate;
    protected boolean trace;
    protected Date beginDate;
    protected Date endDate;
    protected String visibility;
    protected String logicName;
    protected String systemFrom;
    protected String pool;
    protected boolean isMaxConcurrentTasksOverridden;
    protected int maxConcurrentTasks;
    protected boolean expandFields;
    protected boolean expandValues;
    protected Map<String,List<String>> requestHeaders;
    
    public DefaultQueryParameters() {
        clear();
    }
    
    /**
     * Configure internal variables via the incoming parameter map, performing validation of values.
     *
     * QueryParameters are considered valid if the following required parameters are present.
     * <ol>
     * <li>'query'</li>
     * <li>'queryName'</li>
     * <li>'persistence'</li>
     * <li>'auths'</li>
     * <li>'expiration'</li>
     * <li>'queryLogicName'</li>
     * </ol>
     *
     * QueryParameters may also include the following optional parameters.
     * <ol>
     * <li>'pagesize'</li>
     * <li>'pageTimeout'</li>
     * <li>'begin'</li>
     * <li>'end'</li>
     * </ol>
     *
     * @param parameters
     *            - a Map of QueryParameters
     * @throws IllegalArgumentException
     *             when a bad argument is encountered
     */
    public void validate(Map<String,List<String>> parameters) throws IllegalArgumentException {
        for (String param : KNOWN_PARAMS) {
            List<String> values = parameters.get(param);
            if (null == values) {
                continue;
            }
            if (values.isEmpty() || values.size() > 1) {
                throw new IllegalArgumentException("Known parameter [" + param + "] only accepts one value");
            }
            if (QUERY_STRING.equals(param)) {
                this.query = values.get(0);
            } else if (QUERY_NAME.equals(param)) {
                this.queryName = values.get(0);
            } else if (QUERY_PERSISTENCE.equals(param)) {
                this.persistenceMode = QueryPersistence.valueOf(values.get(0));
            } else if (QUERY_PAGESIZE.equals(param)) {
                this.pagesize = Integer.parseInt(values.get(0));
            } else if (QUERY_PAGETIMEOUT.equals(param)) {
                this.pageTimeout = Integer.parseInt(values.get(0));
            } else if (QUERY_MAX_RESULTS_OVERRIDE.equals(param)) {
                this.maxResultsOverride = Long.parseLong(values.get(0));
                this.isMaxResultsOverridden = true;
            } else if (QUERY_MAX_CONCURRENT_TASKS.equals(param)) {
                this.maxConcurrentTasks = Integer.parseInt(values.get(0));
                this.isMaxConcurrentTasksOverridden = true;
            } else if (QUERY_AUTHORIZATIONS.equals(param)) {
                // ensure that auths are comma separated with no empty values or spaces
                Splitter splitter = Splitter.on(',').omitEmptyStrings().trimResults();
                this.auths = StringUtils.join(splitter.splitToList(values.get(0)), ",");
            } else if (QUERY_EXPIRATION.equals(param)) {
                try {
                    this.expirationDate = parseEndDate(values.get(0));
                } catch (ParseException e) {
                    throw new IllegalArgumentException("Error parsing expiration date", e);
                }
            } else if (QUERY_TRACE.equals(param)) {
                this.trace = Boolean.parseBoolean(values.get(0));
            } else if (QUERY_BEGIN.equals(param)) {
                try {
                    this.beginDate = values.get(0) == null ? null : parseStartDate(values.get(0));
                } catch (ParseException e) {
                    throw new IllegalArgumentException("Error parsing begin date", e);
                }
            } else if (QUERY_END.equals(param)) {
                try {
                    this.endDate = values.get(0) == null ? null : parseEndDate(values.get(0));
                } catch (ParseException e) {
                    throw new IllegalArgumentException("Error parsing end date", e);
                }
            } else if (QUERY_VISIBILITY.equals(param)) {
                this.visibility = values.get(0);
            } else if (QUERY_LOGIC_NAME.equals(param)) {
                this.logicName = values.get(0);
            } else if (QUERY_POOL.equals(param)) {
                this.pool = values.get(0);
            } else if (QUERY_PLAN_EXPAND_FIELDS.equals(param)) {
                this.expandFields = Boolean.parseBoolean(values.get(0));
            } else if (QUERY_PLAN_EXPAND_VALUES.equals(param)) {
                this.expandValues = Boolean.parseBoolean(values.get(0));
            } else if (QUERY_SYSTEM_FROM.equals(param)) {
                this.systemFrom = values.get(0);
            } else {
                throw new IllegalArgumentException("Unknown condition.");
            }
        }
        
        try {
            Preconditions.checkNotNull(this.query, "QueryParameter 'query' cannot be null");
            Preconditions.checkNotNull(this.queryName, "QueryParameter 'queryName' cannot be null");
            Preconditions.checkNotNull(this.persistenceMode, "QueryParameter 'persistence' mode cannot be null");
            Preconditions.checkNotNull(this.auths, "QueryParameter 'auths' cannot be null");
            Preconditions.checkNotNull(this.expirationDate, "QueryParameter 'expirationDate' cannot be null");
            Preconditions.checkNotNull(this.logicName, "QueryParameter 'logicName' cannot be null");
        } catch (NullPointerException e) {
            throw new IllegalArgumentException("Missing one or more required QueryParameters", e);
        }
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        
        DefaultQueryParameters that = (DefaultQueryParameters) o;
        
        if (pagesize != that.pagesize)
            return false;
        if (pageTimeout != that.pageTimeout)
            return false;
        if (isMaxResultsOverridden != that.isMaxResultsOverridden)
            return false;
        if (isMaxResultsOverridden) {
            if (maxResultsOverride != that.maxResultsOverride)
                return false;
        }
        if (isMaxConcurrentTasksOverridden != that.isMaxConcurrentTasksOverridden)
            return false;
        if (maxConcurrentTasks != that.maxConcurrentTasks)
            return false;
        if (expandFields != that.expandFields)
            return false;
        if (expandValues != that.expandValues)
            return false;
        if (trace != that.trace)
            return false;
        if (!auths.equals(that.auths))
            return false;
        if (beginDate != null ? !beginDate.equals(that.beginDate) : that.beginDate != null)
            return false;
        if (visibility != null ? !visibility.equals(that.visibility) : that.visibility != null)
            return false;
        if (endDate != null ? !endDate.equals(that.endDate) : that.endDate != null)
            return false;
        if (!expirationDate.equals(that.expirationDate))
            return false;
        if (logicName != null ? !logicName.equals(that.logicName) : that.logicName != null)
            return false;
        if (pool != null ? !pool.equals(that.pool) : that.pool != null)
            return false;
        if (persistenceMode != that.persistenceMode)
            return false;
        if (!query.equals(that.query))
            return false;
        if (!queryName.equals(that.queryName))
            return false;
        if (requestHeaders != null ? !requestHeaders.equals(that.requestHeaders) : that.requestHeaders != null)
            return false;
        if (systemFrom != null ? !systemFrom.equals(that.systemFrom) : that.systemFrom != null)
            return false;
        return true;
    }
    
    @Override
    public int hashCode() {
        int result = query.hashCode();
        result = 31 * result + queryName.hashCode();
        result = 31 * result + persistenceMode.hashCode();
        result = 31 * result + pagesize;
        result = 31 * result + pageTimeout;
        if (isMaxResultsOverridden) {
            result = 31 * result + (int) (maxResultsOverride);
        }
        if (isMaxConcurrentTasksOverridden) {
            result = 31 * result + maxConcurrentTasks;
        }
        result = 31 * result + Boolean.hashCode(expandFields);
        result = 31 * result + Boolean.hashCode(expandValues);
        result = 31 * result + auths.hashCode();
        result = 31 * result + expirationDate.hashCode();
        result = 31 * result + (trace ? 1 : 0);
        result = 31 * result + (beginDate != null ? beginDate.hashCode() : 0);
        result = 31 * result + (endDate != null ? endDate.hashCode() : 0);
        result = 31 * result + (visibility != null ? visibility.hashCode() : 0);
        result = 31 * result + (logicName != null ? logicName.hashCode() : 0);
        result = 31 * result + (pool != null ? pool.hashCode() : 0);
        result = 31 * result + (requestHeaders != null ? requestHeaders.hashCode() : 0);
        result = 31 * result + (systemFrom != null ? systemFrom.hashCode() : 0);
        return result;
    }
    
    public static synchronized String formatDate(Date d) throws ParseException {
        String formatPattern = "yyyyMMdd HHmmss.SSS";
        SimpleDateFormat formatter = new SimpleDateFormat(formatPattern);
        formatter.setLenient(false);
        return formatter.format(d);
    }
    
    protected static final String defaultStartTime = "000000";
    protected static final String defaultStartMillisec = "000";
    protected static final String defaultEndTime = "235959";
    protected static final String defaultEndMillisec = "999";
    protected static final String formatPattern = "yyyyMMdd HHmmss.SSS";
    private static final SimpleDateFormat dateFormat;
    
    static {
        dateFormat = new SimpleDateFormat(formatPattern);
        dateFormat.setLenient(false);
    }
    
    public static Date parseStartDate(String s) throws ParseException {
        return parseDate(s, defaultStartTime, defaultStartMillisec);
    }
    
    public static Date parseEndDate(String s) throws ParseException {
        return parseDate(s, defaultEndTime, defaultEndMillisec);
    }
    
    public static synchronized Date parseDate(String s, String defaultTime, String defaultMillisec) throws ParseException {
        Date d;
        ParseException e = null;
        synchronized (DefaultQueryParameters.dateFormat) {
            String str = s;
            if (str.equals("+24Hours")) {
                d = DateUtils.addDays(new Date(), 1);
            } else {
                if (StringUtils.isNotBlank(defaultTime) && !str.contains(" ")) {
                    str = str + " " + defaultTime;
                }
                
                if (StringUtils.isNotBlank(defaultMillisec) && !str.contains(".")) {
                    str = str + "." + defaultMillisec;
                }
                
                try {
                    d = DefaultQueryParameters.dateFormat.parse(str);
                    // if any time value in HHmmss was set either by default or by the user
                    // then we want to include ALL of that second by setting the milliseconds to 999
                    if (DateUtils.getFragmentInMilliseconds(d, Calendar.HOUR_OF_DAY) > 0) {
                        DateUtils.setMilliseconds(d, 999);
                    }
                } catch (ParseException pe) {
                    throw new RuntimeException("Unable to parse date " + str + " with format " + formatPattern, e);
                }
            }
        }
        return d;
    }
    
    /**
     * Convenience method to generate a {@code Map<String,List<String>>} from the specified arguments. If an argument is null, it's associated parameter name
     * (key) will not be added to the map, which is why Integer and Boolean wrappers are used for greater flexibility.
     *
     * The 'parameters' argument will not be parsed, so its internal elements will not be placed into the map. If non-null, the 'parameters' value will be
     * mapped directly to the QUERY_PARAMS key.
     *
     * No attempt is made to determine whether or not the given arguments constitute a valid query. If validation is desired, see the {@link #validate(Map)}
     * method
     *
     * @param queryLogicName
     *            - name of QueryLogic to use
     * @param query
     *            - the raw query string
     * @param queryName
     *            - client-supplied name of query
     * @param queryVisibility
     *            - query
     * @param beginDate
     *            - start date
     * @param endDate
     *            - end date
     * @param queryAuthorizations
     *            - what auths the query should run with
     * @param expirationDate
     *            - expiration date
     * @param pagesize
     *            - page size
     * @param pageTimeout
     *            - page timeout
     * @param maxResultsOverride
     *            - max results override
     * @param persistenceMode
     *            - persistence mode
     * @param systemFrom
     *            - system from
     * @param parameters
     *            - additional parameters passed in as map
     * @param trace
     *            - trace flag
     * @return parameter map
     * @throws ParseException
     *             on date parse/format error
     */
    public static Map<String,List<String>> paramsToMap(String queryLogicName, String query, String queryName, String queryVisibility, Date beginDate,
                    Date endDate, String queryAuthorizations, Date expirationDate, Integer pagesize, Integer pageTimeout, Long maxResultsOverride,
                    QueryPersistence persistenceMode, String systemFrom, String parameters, Boolean trace) throws ParseException {
        
        MultiValueMap<String,String> p = new LinkedMultiValueMap<>();
        if (queryLogicName != null) {
            p.set(QueryParameters.QUERY_LOGIC_NAME, queryLogicName);
        }
        if (query != null) {
            p.set(QueryParameters.QUERY_STRING, query);
        }
        if (queryName != null) {
            p.set(QueryParameters.QUERY_NAME, queryName);
        }
        if (queryVisibility != null) {
            p.set(QueryParameters.QUERY_VISIBILITY, queryVisibility);
        }
        if (beginDate != null) {
            p.set(QueryParameters.QUERY_BEGIN, formatDate(beginDate));
        }
        if (endDate != null) {
            p.set(QueryParameters.QUERY_END, formatDate(endDate));
        }
        if (queryAuthorizations != null) {
            // ensure that auths are comma separated with no empty values or spaces
            Splitter splitter = Splitter.on(',').omitEmptyStrings().trimResults();
            p.set(QueryParameters.QUERY_AUTHORIZATIONS, StringUtils.join(splitter.splitToList(queryAuthorizations), ","));
        }
        if (expirationDate != null) {
            p.set(QueryParameters.QUERY_EXPIRATION, formatDate(expirationDate));
        }
        if (pagesize != null) {
            p.set(QueryParameters.QUERY_PAGESIZE, pagesize.toString());
        }
        if (pageTimeout != null) {
            p.set(QueryParameters.QUERY_PAGETIMEOUT, pageTimeout.toString());
        }
        if (maxResultsOverride != null) {
            p.set(QueryParameters.QUERY_MAX_RESULTS_OVERRIDE, maxResultsOverride.toString());
        }
        if (persistenceMode != null) {
            p.set(QueryParameters.QUERY_PERSISTENCE, persistenceMode.name());
        }
        if (trace != null) {
            p.set(QueryParameters.QUERY_TRACE, trace.toString());
        }
        if (systemFrom != null) {
            p.set(QueryParameters.QUERY_SYSTEM_FROM, systemFrom);
        }
        if (parameters != null) {
            p.set(QueryParameters.QUERY_PARAMS, parameters);
        }
        
        return p;
    }
    
    @Override
    public String getQuery() {
        return query;
    }
    
    @Override
    public void setQuery(String query) {
        this.query = query;
    }
    
    @Override
    public String getQueryName() {
        return queryName;
    }
    
    @Override
    public void setQueryName(String queryName) {
        this.queryName = queryName;
    }
    
    @Override
    public QueryPersistence getPersistenceMode() {
        return persistenceMode;
    }
    
    @Override
    public void setPersistenceMode(QueryPersistence persistenceMode) {
        this.persistenceMode = persistenceMode;
    }
    
    @Override
    public int getPagesize() {
        return pagesize;
    }
    
    @Override
    public void setPagesize(int pagesize) {
        this.pagesize = pagesize;
    }
    
    @Override
    public int getPageTimeout() {
        return pageTimeout;
    }
    
    @Override
    public void setPageTimeout(int pageTimeout) {
        this.pageTimeout = pageTimeout;
    }
    
    @Override
    public long getMaxResultsOverride() {
        return maxResultsOverride;
    }
    
    @Override
    public void setMaxResultsOverride(long maxResultsOverride) {
        this.maxResultsOverride = maxResultsOverride;
    }
    
    @Override
    public boolean isMaxResultsOverridden() {
        return this.isMaxResultsOverridden;
    }
    
    @Override
    public String getAuths() {
        return auths;
    }
    
    @Override
    public void setAuths(String auths) {
        this.auths = auths;
    }
    
    @Override
    public Date getExpirationDate() {
        return expirationDate;
    }
    
    @Override
    public void setExpirationDate(Date expirationDate) {
        this.expirationDate = expirationDate;
    }
    
    @Override
    public boolean isTrace() {
        return trace;
    }
    
    @Override
    public void setTrace(boolean trace) {
        this.trace = trace;
    }
    
    @Override
    public Date getBeginDate() {
        return beginDate;
    }
    
    @Override
    public Date getEndDate() {
        return endDate;
    }
    
    @Override
    public void setBeginDate(Date beginDate) {
        this.beginDate = beginDate;
    }
    
    @Override
    public void setEndDate(Date endDate) {
        this.endDate = endDate;
    }
    
    @Override
    public String getVisibility() {
        return visibility;
    }
    
    @Override
    public void setVisibility(String visibility) {
        this.visibility = visibility;
    }
    
    @Override
    public String getLogicName() {
        return logicName;
    }
    
    @Override
    public void setLogicName(String logicName) {
        this.logicName = logicName;
    }
    
    @Override
    public String getSystemFrom() {
        return systemFrom;
    }
    
    @Override
    public void setSystemFrom(String systemFrom) {
        this.systemFrom = systemFrom;
    }
    
    @Override
    public String getPool() {
        return pool;
    }
    
    @Override
    public void setPool(String pool) {
        this.pool = pool;
    }
    
    @Override
    public int getMaxConcurrentTasks() {
        return maxConcurrentTasks;
    }
    
    @Override
    public void setMaxConcurrentTasks(int maxConcurrentTasks) {
        this.maxConcurrentTasks = maxConcurrentTasks;
    }
    
    @Override
    public boolean isMaxConcurrentTasksOverridden() {
        return isMaxConcurrentTasksOverridden;
    }
    
    @Override
    public boolean isExpandFields() {
        return expandFields;
    }
    
    @Override
    public void setExpandFields(boolean expandFields) {
        this.expandFields = expandFields;
    }
    
    @Override
    public boolean isExpandValues() {
        return expandValues;
    }
    
    @Override
    public void setExpandValues(boolean expandValues) {
        this.expandValues = expandValues;
    }
    
    @Override
    public Map<String,List<String>> getRequestHeaders() {
        return requestHeaders;
    }
    
    @Override
    public void setRequestHeaders(Map<String,List<String>> requestHeaders) {
        this.requestHeaders = requestHeaders;
    }
    
    @Override
    public MultiValueMap<String,String> getUnknownParameters(Map<String,List<String>> allQueryParameters) {
        MultiValueMap<String,String> p = new LinkedMultiValueMap<>();
        for (String key : allQueryParameters.keySet()) {
            if (!KNOWN_PARAMS.contains(key)) {
                p.put(key, allQueryParameters.get(key));
            }
        }
        return p;
    }
    
    @Override
    public void clear() {
        this.query = null;
        this.queryName = null;
        this.persistenceMode = QueryPersistence.TRANSIENT;
        this.pagesize = 10;
        this.pageTimeout = -1;
        this.isMaxResultsOverridden = false;
        this.auths = null;
        this.expirationDate = DateUtils.addDays(new Date(), 1);
        this.trace = false;
        this.beginDate = null;
        this.endDate = null;
        this.visibility = null;
        this.logicName = null;
        this.pool = null;
        this.isMaxConcurrentTasksOverridden = false;
        this.maxConcurrentTasks = 0;
        this.expandFields = true;
        this.expandValues = false;
        this.requestHeaders = null;
        this.systemFrom = null;
    }
}
