package datawave.core.query.logic;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.springframework.beans.factory.annotation.Required;

import datawave.audit.SelectorExtractor;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.iterator.DatawaveTransformIterator;
import datawave.marking.MarkingFunctions;
import datawave.microservice.query.Query;
import datawave.security.authorization.ProxiedUserDetails;
import datawave.security.authorization.UserOperations;
import datawave.webservice.common.audit.Auditor.AuditType;
import datawave.webservice.common.connection.AccumuloClientConfiguration;
import datawave.webservice.query.result.event.ResponseObjectFactory;

public abstract class BaseQueryLogic<T> implements QueryLogic<T> {

    private GenericQueryConfiguration baseConfig;
    private String logicName = "No logicName was set";
    private String logicDescription = "Not configured";
    private AuditType auditType = null;
    private Map<String,Long> dnResultLimits = null;
    private Map<String,Long> systemFromResultLimits = null;
    protected long maxResults = -1L;
    protected int maxConcurrentTasks = -1;
    protected ScannerBase scanner;
    @SuppressWarnings("unchecked")
    protected Iterator<T> iterator = (Iterator<T>) Collections.emptyList().iterator();
    private int maxPageSize = 0;
    private long pageByteTrigger = 0;
    private boolean collectQueryMetrics = true;
    private String _connPoolName;
    private Set<String> authorizedDNs;

    protected ProxiedUserDetails currentUser;
    protected ProxiedUserDetails serverUser;

    protected Set<String> requiredRoles;
    protected MarkingFunctions markingFunctions;
    protected ResponseObjectFactory responseObjectFactory;
    protected SelectorExtractor selectorExtractor;
    protected ResponseEnricherBuilder responseEnricherBuilder = null;
    protected AccumuloClientConfiguration clientConfig = null;

    public static final String BYPASS_ACCUMULO = "rfile.debug";

    public BaseQueryLogic() {
        getConfig().setBaseIteratorPriority(100);
    }

    public BaseQueryLogic(BaseQueryLogic<T> other) {
        // copy base config variables
        this.baseConfig = new GenericQueryConfiguration(other.getConfig());

        // copy other variables
        setMaxResults(other.maxResults);
        setMarkingFunctions(other.getMarkingFunctions());
        setResponseObjectFactory(other.getResponseObjectFactory());
        setLogicName(other.getLogicName());
        setLogicDescription(other.getLogicDescription());
        setAuditType(other.getAuditType(null));
        this.dnResultLimits = other.dnResultLimits;
        this.systemFromResultLimits = other.systemFromResultLimits;
        this.scanner = other.scanner;
        this.iterator = other.iterator;
        setMaxPageSize(other.getMaxPageSize());
        setPageByteTrigger(other.getPageByteTrigger());
        setCollectQueryMetrics(other.getCollectQueryMetrics());
        this.authorizedDNs = other.authorizedDNs;
        setConnPoolName(other.getConnPoolName());
        setRequiredRoles(other.getRequiredRoles());
        setSelectorExtractor(other.getSelectorExtractor());
        setCurrentUser(other.getCurrentUser());
        setServerUser(other.getServerUser());
        setResponseEnricherBuilder(other.getResponseEnricherBuilder());
    }

    public GenericQueryConfiguration getConfig() {
        if (baseConfig == null) {
            baseConfig = new GenericQueryConfiguration() {};
        }

        return baseConfig;
    }

    @Override
    public String getPlan(AccumuloClient client, Query settings, Set<Authorizations> runtimeQueryAuthorizations, boolean expandFields, boolean expandValues)
                    throws Exception {
        // for many query logics, the query is what it is
        return settings.getQuery();
    }

    public MarkingFunctions getMarkingFunctions() {
        return markingFunctions;
    }

    public void setMarkingFunctions(MarkingFunctions markingFunctions) {
        this.markingFunctions = markingFunctions;
    }

    public ResponseObjectFactory getResponseObjectFactory() {
        return responseObjectFactory;
    }

    public void setResponseObjectFactory(ResponseObjectFactory responseObjectFactory) {
        this.responseObjectFactory = responseObjectFactory;
    }

    public ProxiedUserDetails getCurrentUser() {
        return currentUser;
    }

    public void setCurrentUser(ProxiedUserDetails currentUser) {
        this.currentUser = currentUser;
    }

    public ProxiedUserDetails getServerUser() {
        return serverUser;
    }

    public void setServerUser(ProxiedUserDetails serverUser) {
        this.serverUser = serverUser;
    }

    public Set<String> getRequiredRoles() {
        return requiredRoles;
    }

    public void setRequiredRoles(Set<String> requiredRoles) {
        this.requiredRoles = requiredRoles;
    }

    @Override
    public String getTableName() {
        return getConfig().getTableName();
    }

    @Override
    public long getMaxResults() {
        return this.maxResults;
    }

    @Override
    public int getMaxConcurrentTasks() {
        return this.maxConcurrentTasks;
    }

    @Override
    @Deprecated
    public long getMaxRowsToScan() {
        return getMaxWork();
    }

    @Override
    public long getMaxWork() {
        return getConfig().getMaxWork();
    }

    @Override
    public void setTableName(String tableName) {
        getConfig().setTableName(tableName);
    }

    @Override
    public void setMaxResults(long maxResults) {
        this.maxResults = maxResults;
    }

    @Override
    public void setMaxConcurrentTasks(int maxConcurrentTasks) {
        this.maxConcurrentTasks = maxConcurrentTasks;
    }

    @Override
    @Deprecated
    public void setMaxRowsToScan(long maxRowsToScan) {
        setMaxWork(maxRowsToScan);
    }

    @Override
    public void setMaxWork(long maxWork) {
        getConfig().setMaxWork(maxWork);
    }

    @Override
    public int getMaxPageSize() {
        return maxPageSize;
    }

    @Override
    public void setMaxPageSize(int maxPageSize) {
        this.maxPageSize = maxPageSize;
    }

    @Override
    public long getPageByteTrigger() {
        return pageByteTrigger;
    }

    @Override
    public void setPageByteTrigger(long pageByteTrigger) {
        this.pageByteTrigger = pageByteTrigger;
    }

    @Override
    public int getBaseIteratorPriority() {
        return getConfig().getBaseIteratorPriority();
    }

    @Override
    public void setBaseIteratorPriority(final int baseIteratorPriority) {
        getConfig().setBaseIteratorPriority(baseIteratorPriority);
    }

    @Override
    public Iterator<T> iterator() {
        return iterator;
    }

    @Override
    public final QueryLogicTransformer getEnrichedTransformer(Query settings) {
        QueryLogicTransformer transformer = this.getTransformer(settings);
        if (responseEnricherBuilder != null) {
            //@formatter:off
            ResponseEnricher enricher = responseEnricherBuilder
                    .withConfig(getConfig())
                    .withMarkingFunctions(getMarkingFunctions())
                    .withResponseObjectFactory(responseObjectFactory)
                    .withCurrentUser(getCurrentUser())
                    .withServerUser(getServerUser())
                    .build();
            //@formatter:on
            transformer.setResponseEnricher(enricher);
        }
        return transformer;
    }

    @Override
    public TransformIterator getTransformIterator(Query settings) {
        return new DatawaveTransformIterator(this.iterator(), this.getTransformer(settings));
    }

    @Override
    public String getLogicName() {
        return logicName;
    }

    @Override
    public void setLogicName(String logicName) {
        this.logicName = logicName;
    }

    public boolean getBypassAccumulo() {
        return getConfig().getBypassAccumulo();
    }

    public void setBypassAccumulo(boolean bypassAccumulo) {
        getConfig().setBypassAccumulo(bypassAccumulo);
    }

    public String getAccumuloPassword() {
        return getConfig().getAccumuloPassword();
    }

    public void setAccumuloPassword(String accumuloPassword) {
        getConfig().setAccumuloPassword(accumuloPassword);
    }

    @Override
    public abstract Object clone() throws CloneNotSupportedException;

    public void close() {
        if (null == scanner)
            return;
        if (scanner instanceof BatchScanner) {
            scanner.close();
        }
    }

    /*
     * Implementations must override if they want a query-specific value returned
     */
    @Override
    public AuditType getAuditType(Query query) {
        return auditType;
    }

    @Override
    public AuditType getAuditType() {
        return auditType;
    }

    @Override
    @Required
    // enforces that the unit tests will fail and the application will not deploy unless this property is set
    public void setAuditType(AuditType auditType) {
        this.auditType = auditType;
    }

    @Override
    public void setLogicDescription(String logicDescription) {
        this.logicDescription = logicDescription;
    }

    @Override
    public String getLogicDescription() {
        return logicDescription;
    }

    public boolean getCollectQueryMetrics() {
        return collectQueryMetrics;
    }

    public void setCollectQueryMetrics(boolean collectQueryMetrics) {
        this.collectQueryMetrics = collectQueryMetrics;
    }

    /** {@inheritDoc} */
    @Override
    public String getConnPoolName() {
        return _connPoolName;
    }

    /** {@inheritDoc} */
    @Override
    public void setConnPoolName(final String connPoolName) {
        _connPoolName = connPoolName;
    }

    /** {@inheritDoc} */
    public boolean canRunQuery(Collection<String> userRoles) {
        return this.requiredRoles == null || userRoles.containsAll(requiredRoles);
    }

    @Override
    public final void validate(Map<String,List<String>> parameters) throws IllegalArgumentException {
        Set<String> requiredParams = getRequiredQueryParameters();
        for (String required : requiredParams) {
            List<String> values = parameters.get(required);
            if (null == values) {
                throw new IllegalArgumentException("Required parameter " + required + " not found");
            }
        }
    }

    @Override
    public List<String> getSelectors(Query settings) throws IllegalArgumentException {
        List<String> selectorList = null;
        if (this.selectorExtractor != null) {
            try {
                selectorList = this.selectorExtractor.extractSelectors(settings);
            } catch (Exception e) {
                throw new IllegalArgumentException(e.getMessage(), e);
            }
        }
        return selectorList;
    }

    public void setSelectorExtractor(SelectorExtractor selectorExtractor) {
        this.selectorExtractor = selectorExtractor;
    }

    public SelectorExtractor getSelectorExtractor() {
        return selectorExtractor;
    }

    @Override
    public Set<String> getAuthorizedDNs() {
        return authorizedDNs;
    }

    @Override
    public void setAuthorizedDNs(Set<String> authorizedDNs) {
        this.authorizedDNs = authorizedDNs;
    }

    @Override
    public void setDnResultLimits(Map<String,Long> dnResultLimits) {
        this.dnResultLimits = dnResultLimits;
    }

    @Override
    public Map<String,Long> getDnResultLimits() {
        return dnResultLimits;
    }

    @Override
    public void setSystemFromResultLimits(Map<String,Long> systemFromLimits) {
        this.systemFromResultLimits = systemFromLimits;
    }

    @Override
    public Map<String,Long> getSystemFromResultLimits() {
        return systemFromResultLimits;
    }

    @Override
    public void setPageProcessingStartTime(long pageProcessingStartTime) {
        // no op
    }

    /**
     * Whether the query is a type that should be allowed to be run long (exceed the short circuit timeout). Default to false. Implementations should override
     * this if the default is not appropriate.
     *
     * @return Return whether the query is a type that should be allowed to be run long (exceed the short circuit timeout)
     */
    @Override
    public boolean isLongRunningQuery() {
        return false;
    }

    public ResponseEnricherBuilder getResponseEnricherBuilder() {
        return responseEnricherBuilder;
    }

    public void setResponseEnricherBuilder(ResponseEnricherBuilder responseEnricherBuilder) {
        this.responseEnricherBuilder = responseEnricherBuilder;
    }

    @Override
    public UserOperations getUserOperations() {
        // null implies that the local user operations/principal is to be used for auths.
        return null;
    }

    @Override
    public void setClientConfig(AccumuloClientConfiguration clientConfig) {
        this.clientConfig = clientConfig;
    }

    @Override
    public AccumuloClientConfiguration getClientConfig() {
        return clientConfig;
    }

    public Map<String,ScannerBase.ConsistencyLevel> getConsistencyLevels() {
        return getConfig().getConsistencyLevels();
    }

    public void setConsistencyLevels(Map<String,ScannerBase.ConsistencyLevel> consistencyLevels) {
        getConfig().setConsistencyLevels(consistencyLevels);
    }

    public Map<String,Map<String,String>> getHints() {
        return getConfig().getHints();
    }

    public void setHints(Map<String,Map<String,String>> hints) {
        getConfig().setHints(hints);
    }
}
