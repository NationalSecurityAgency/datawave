package datawave.webservice.query.logic;

import datawave.audit.SelectorExtractor;
import datawave.marking.MarkingFunctions;
import datawave.webservice.common.audit.Auditor;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.security.authorization.UserOperations;
import datawave.webservice.query.Query;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.collections4.iterators.TransformIterator;

import java.security.Principal;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A delegating query logic that simply passes through to a delegate query logic. Intended to simplify extending classes.
 */
public abstract class DelegatingQueryLogic implements QueryLogic<Object> {
    
    private QueryLogic<Object> delegate;
    
    public DelegatingQueryLogic() {}
    
    public DelegatingQueryLogic(DelegatingQueryLogic other) throws CloneNotSupportedException {
        this.delegate = (QueryLogic<Object>) (other.delegate.clone());
    }
    
    public QueryLogic<Object> getDelegate() {
        return delegate;
    }
    
    public void setDelegate(QueryLogic<Object> delegate) {
        this.delegate = delegate;
    }
    
    @Override
    public String getPlan(AccumuloClient connection, Query settings, Set<Authorizations> runtimeQueryAuthorizations, boolean expandFields, boolean expandValues)
                    throws Exception {
        return delegate.getPlan(connection, settings, runtimeQueryAuthorizations, expandFields, expandValues);
    }
    
    @Override
    public GenericQueryConfiguration initialize(AccumuloClient connection, Query settings, Set<Authorizations> runtimeQueryAuthorizations) throws Exception {
        return delegate.initialize(connection, settings, runtimeQueryAuthorizations);
    }
    
    @Override
    public void setupQuery(GenericQueryConfiguration configuration) throws Exception {
        delegate.setupQuery(configuration);
    }
    
    @Override
    public Iterator<Object> iterator() {
        return delegate.iterator();
    }
    
    public List<String> getSelectors(Query settings) {
        return delegate.getSelectors(settings);
    }
    
    @Override
    public SelectorExtractor getSelectorExtractor() {
        return delegate.getSelectorExtractor();
    }
    
    @Override
    public abstract Object clone() throws CloneNotSupportedException;
    
    @Override
    public AccumuloConnectionFactory.Priority getConnectionPriority() {
        return delegate.getConnectionPriority();
    }
    
    @Override
    public QueryLogicTransformer getTransformer(Query settings) {
        return delegate.getTransformer(settings);
    }
    
    @Override
    public final QueryLogicTransformer getEnrichedTransformer(Query settings) {
        return delegate.getEnrichedTransformer(settings);
    }
    
    @Override
    public String getResponseClass(Query query) throws QueryException {
        return delegate.getResponseClass(query);
    }
    
    @Override
    public TransformIterator getTransformIterator(Query settings) {
        return delegate.getTransformIterator(settings);
    }
    
    @Override
    public boolean isLongRunningQuery() {
        return delegate.isLongRunningQuery();
    }
    
    @Override
    public void close() {
        delegate.close();
    }
    
    @Override
    public String getTableName() {
        return delegate.getTableName();
    }
    
    @Override
    public long getMaxResults() {
        return delegate.getMaxResults();
    }
    
    @Override
    @Deprecated
    public long getMaxRowsToScan() {
        return delegate.getMaxRowsToScan();
    }
    
    @Override
    public long getMaxWork() {
        return delegate.getMaxWork();
    }
    
    @Override
    public int getMaxPageSize() {
        return delegate.getMaxPageSize();
    }
    
    @Override
    public long getPageByteTrigger() {
        return delegate.getPageByteTrigger();
    }
    
    @Override
    public int getBaseIteratorPriority() {
        return delegate.getBaseIteratorPriority();
    }
    
    @Override
    public void setTableName(String tableName) {
        delegate.setTableName(tableName);
    }
    
    @Override
    public void setMaxResults(long maxResults) {
        delegate.setMaxResults(maxResults);
    }
    
    @Override
    @Deprecated
    public void setMaxRowsToScan(long maxRowsToScan) {
        delegate.setMaxRowsToScan(maxRowsToScan);
    }
    
    @Override
    public void setMaxWork(long maxWork) {
        delegate.setMaxWork(maxWork);
    }
    
    @Override
    public void setMaxPageSize(int maxPageSize) {
        delegate.setMaxPageSize(maxPageSize);
    }
    
    @Override
    public void setPageByteTrigger(long pageByteTrigger) {
        delegate.setPageByteTrigger(pageByteTrigger);
    }
    
    @Override
    public void setBaseIteratorPriority(int priority) {
        delegate.setBaseIteratorPriority(priority);
    }
    
    @Override
    public void setLogicName(String logicName) {
        delegate.setLogicName(logicName);
    }
    
    @Override
    public String getLogicName() {
        return delegate.getLogicName();
    }
    
    @Override
    public void setLogicDescription(String logicDescription) {
        delegate.setLogicDescription(logicDescription);
    }
    
    @Override
    public Auditor.AuditType getAuditType(Query query) {
        return delegate.getAuditType(query);
    }
    
    @Override
    public Auditor.AuditType getAuditType() {
        return delegate.getAuditType();
    }
    
    @Override
    public void setAuditType(Auditor.AuditType auditType) {
        delegate.setAuditType(auditType);
    }
    
    @Override
    public String getLogicDescription() {
        return delegate.getLogicDescription();
    }
    
    @Override
    public boolean getCollectQueryMetrics() {
        return delegate.getCollectQueryMetrics();
    }
    
    @Override
    public void setCollectQueryMetrics(boolean collectQueryMetrics) {
        delegate.setCollectQueryMetrics(collectQueryMetrics);
    }
    
    @Override
    public void setRoleManager(RoleManager roleManager) {
        delegate.setRoleManager(roleManager);
    }
    
    @Override
    public RoleManager getRoleManager() {
        return delegate.getRoleManager();
    }
    
    @Override
    public Set<String> getOptionalQueryParameters() {
        return delegate.getOptionalQueryParameters();
    }
    
    @Override
    public void setConnPoolName(String connPoolName) {
        delegate.setConnPoolName(connPoolName);
    }
    
    @Override
    public String getConnPoolName() {
        return delegate.getConnPoolName();
    }
    
    @Override
    public boolean canRunQuery(Principal principal) {
        return delegate.canRunQuery(principal);
    }
    
    @Override
    public boolean canRunQuery() {
        return delegate.canRunQuery();
    }
    
    @Override
    public void setPrincipal(Principal principal) {
        delegate.setPrincipal(principal);
    }
    
    @Override
    public Principal getPrincipal() {
        return delegate.getPrincipal();
    }
    
    @Override
    public MarkingFunctions getMarkingFunctions() {
        return delegate.getMarkingFunctions();
    }
    
    @Override
    public void setMarkingFunctions(MarkingFunctions markingFunctions) {
        delegate.setMarkingFunctions(markingFunctions);
    }
    
    @Override
    public ResponseObjectFactory getResponseObjectFactory() {
        return delegate.getResponseObjectFactory();
    }
    
    @Override
    public void setResponseObjectFactory(ResponseObjectFactory responseObjectFactory) {
        delegate.setResponseObjectFactory(responseObjectFactory);
    }
    
    @Override
    public Set<String> getRequiredQueryParameters() {
        return delegate.getRequiredQueryParameters();
    }
    
    @Override
    public Set<String> getExampleQueries() {
        return delegate.getExampleQueries();
    }
    
    @Override
    public Set<String> getAuthorizedDNs() {
        return delegate.getAuthorizedDNs();
    }
    
    @Override
    public void setAuthorizedDNs(Set<String> allowedDNs) {
        delegate.setAuthorizedDNs(allowedDNs);
    }
    
    @Override
    public boolean containsDNWithAccess(Collection<String> dns) {
        return delegate.containsDNWithAccess(dns);
    }
    
    @Override
    public void setDnResultLimits(Map<String,Long> dnResultLimits) {
        delegate.setDnResultLimits(dnResultLimits);
    }
    
    @Override
    public Map<String,Long> getDnResultLimits() {
        return delegate.getDnResultLimits();
    }
    
    @Override
    public void setSystemFromResultLimits(Map<String,Long> systemFromResultLimits) {
        delegate.setSystemFromResultLimits(systemFromResultLimits);
    }
    
    @Override
    public Map<String,Long> getSystemFromResultLimits() {
        return delegate.getSystemFromResultLimits();
    }
    
    @Override
    public long getResultLimit(Query settings) {
        return delegate.getResultLimit(settings);
    }
    
    @Override
    public void setPageProcessingStartTime(long pageProcessingStartTime) {
        delegate.setPageProcessingStartTime(pageProcessingStartTime);
    }
    
    @Override
    public void validate(Map<String,List<String>> parameters) throws IllegalArgumentException {
        delegate.validate(parameters);
    }
    
    @Override
    public UserOperations getUserOperations() {
        return delegate.getUserOperations();
    }
}
