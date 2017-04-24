package nsa.datawave.webservice.query.logic;

import java.security.Principal;
import java.util.List;
import java.util.Set;

import nsa.datawave.audit.SelectorExtractor;
import nsa.datawave.marking.MarkingFunctions;
import nsa.datawave.validation.ParameterValidator;
import nsa.datawave.webservice.common.audit.Auditor.AuditType;
import nsa.datawave.webservice.common.connection.AccumuloConnectionFactory;
import nsa.datawave.webservice.query.Query;
import nsa.datawave.webservice.query.configuration.GenericQueryConfiguration;
import nsa.datawave.webservice.query.result.event.ResponseObjectFactory;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.collections.iterators.TransformIterator;

public interface QueryLogic<T> extends Iterable<T>, Cloneable, ParameterValidator {
    
    /**
     * Implementations create a configuration using the connection, settings, and runtimeQueryAuthorizations.
     * 
     * @param connection
     *            - Accumulo connector to use for this query
     * @param settings
     *            - query settings (query, begin date, end date, etc.)
     * @param runtimeQueryAuthorizations
     *            - authorizations that have been calculated for this query based on the caller and server.
     * @throws Exception
     */
    public GenericQueryConfiguration initialize(Connector connection, Query settings, Set<Authorizations> runtimeQueryAuthorizations) throws Exception;
    
    /**
     *
     * @param settings
     *            - query settings (query, begin date, end date, etc.)
     * @return list of selectors used in the Query
     */
    public List<String> getSelectors(Query settings);
    
    public SelectorExtractor getSelectorExtractor();
    
    /**
     * Implementations use the configuration to run their query
     * 
     * @param configuration
     *            Encapsulates all information needed to run a query (whether the query is a BatchScanner, a MapReduce job, etc)
     */
    public void setupQuery(GenericQueryConfiguration configuration) throws Exception;
    
    /**
     * @return a copy of this instance
     */
    public Object clone() throws CloneNotSupportedException;
    
    /**
     * @return priority from AccumuloConnectionFactory
     */
    public AccumuloConnectionFactory.Priority getConnectionPriority();
    
    /**
     * @return Transformer that will convert Key,Value to a Result object
     */
    public QueryLogicTransformer getTransformer(Query settings);
    
    /**
     * Allows for the customization of handling query results, e.g. allows for aggregation of query results before returning to the client.
     * 
     * @param settings
     *            The query settings object
     * @return Return a TransformIterator for the QueryLogic implementation
     */
    public TransformIterator getTransformIterator(Query settings);
    
    /**
     * release resources
     */
    public void close();
    
    /** @return the tableName */
    public String getTableName();
    
    /**
     * @return max number of results to pass back to the caller
     */
    public long getMaxResults();
    
    /**
     * @return max number of rows to scan from the iterator
     */
    public long getMaxRowsToScan();
    
    /**
     * @return max number of records to return in a page (max pagesize allowed)
     */
    public int getMaxPageSize();
    
    /**
     * @return the number of bytes at which a page will be returned, even if pagesize has not been reached
     */
    public long getPageByteTrigger();
    
    /**
     * Returns the base iterator priority.
     * 
     * @return base iterator priority
     */
    public int getBaseIteratorPriority();
    
    /**
     * @param tableName
     *            the name of the table
     */
    public void setTableName(String tableName);
    
    /**
     * @param maxResults
     *            max number of results to pass back to the caller
     */
    public void setMaxResults(long maxResults);
    
    /**
     * @param maxRowsToScan
     *            max number of rows to scan from the iterator
     */
    public void setMaxRowsToScan(long maxRowsToScan);
    
    /**
     * @param maxPageSize
     *            max number of records in a page (max pagesize allowed)
     */
    public void setMaxPageSize(int maxPageSize);
    
    /**
     * @param pageByteTrigger
     *            the number of bytes at which a page will be returned, even if pagesize has not been reached
     */
    public void setPageByteTrigger(long pageByteTrigger);
    
    /**
     * Sets the base iterator priority
     * 
     * @param priority
     *            base iterator priority
     */
    public void setBaseIteratorPriority(final int priority);
    
    /**
     * @param logicName
     *            name of the query logic
     */
    public void setLogicName(String logicName);
    
    /**
     * @return name of the query logic
     */
    public String getLogicName();
    
    /**
     * @param logicDescription
     *            a brief description of this logic type
     */
    public void setLogicDescription(String logicDescription);
    
    /**
     * @return the audit level for this logic
     */
    public AuditType getAuditType(Query query);
    
    /**
     * @return the audit level for this logic for a specific query
     */
    public AuditType getAuditType();
    
    /**
     * @param auditType
     *            the audit level for this logic
     */
    public void setAuditType(AuditType auditType);
    
    /**
     * @return a brief description of this logic type
     */
    public String getLogicDescription();
    
    /**
     * @return set of column visibility markings that should not be presented
     */
    public Set<String> getUndisplayedVisibilities();
    
    /**
     * @return should query metrics be collected for this query logic
     */
    public boolean getCollectQueryMetrics();
    
    /**
     * @param collectQueryMetrics
     *            whether query metrics be collected for this query logic
     */
    public void setCollectQueryMetrics(boolean collectQueryMetrics);
    
    void setRoleManager(RoleManager roleManager);
    
    RoleManager getRoleManager();
    
    /**
     * List of parameters that can be used in the 'params' parameter to Query/create
     * 
     * @return the supported parameters
     */
    public Set<String> getOptionalQueryParameters();
    
    /**
     * @param connPoolName
     *            The name of the connection pool to set.
     */
    public void setConnPoolName(String connPoolName);
    
    /** @return the connPoolName */
    public String getConnPoolName();
    
    /**
     * Check that the user has one of the required roles principal my be null when there is no intent to control access to QueryLogic
     * 
     * @param principal
     * @return true/false
     */
    public boolean canRunQuery(Principal principal);
    
    boolean canRunQuery(); // uses member Principal
    
    void setPrincipal(Principal principal);
    
    Principal getPrincipal();
    
    MarkingFunctions getMarkingFunctions();
    
    void setMarkingFunctions(MarkingFunctions markingFunctions);
    
    ResponseObjectFactory getResponseObjectFactory();
    
    void setResponseObjectFactory(ResponseObjectFactory responseObjectFactory);
    
    /**
     * List of parameters that must be passed from the client for this query logic to work
     * 
     * @return the required parameters
     */
    public Set<String> getRequiredQueryParameters();
    
    /**
     * 
     * @return set of example queries
     */
    public Set<String> getExampleQueries();
    
}
