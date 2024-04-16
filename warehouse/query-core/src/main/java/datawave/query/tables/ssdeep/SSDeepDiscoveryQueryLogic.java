package datawave.query.tables.ssdeep;

import java.security.Principal;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.collections4.iterators.TransformIterator;

import datawave.audit.SelectorExtractor;
import datawave.marking.MarkingFunctions;
import datawave.query.discovery.DiscoveryLogic;
import datawave.query.discovery.DiscoveryTransformer;
import datawave.query.model.QueryModel;
import datawave.query.util.MetadataHelperFactory;
import datawave.security.authorization.UserOperations;
import datawave.webservice.common.audit.Auditor;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.query.Query;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.iterator.DatawaveTransformIterator;
import datawave.webservice.query.logic.AbstractQueryLogicTransformer;
import datawave.webservice.query.logic.BaseQueryLogic;
import datawave.webservice.query.logic.QueryLogicTransformer;
import datawave.webservice.query.logic.ResponseEnricherBuilder;
import datawave.webservice.query.logic.RoleManager;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.result.BaseQueryResponse;

/**
 * Implements the SSDeepDiscoveryLogic which will retrieve discovery info for an SSDeep hash. Expected to be used as a part of the SSDeepChainedDiscoveryLogic,
 * this class largely delegates to an instance of the DiscoveryLogic itself, but is required so that we can return DiscoveredSSDeep objects, which encapsulate
 * the DiscoveredThing from the DiscoveryLogic and adds additional information about the query hash that resulted in the retrieval of this hash, including a
 * similarity score. This is performed by the inline transformer class implemented in the getTransformer method.
 * <p>
 * Note: as additional functionality is added to the DiscoveryLogic, delegate methods may need to be added here as well.
 */
public class SSDeepDiscoveryQueryLogic extends BaseQueryLogic<DiscoveredSSDeep> {
    public DiscoveryLogic discoveryDelegate;

    @SuppressWarnings("ConstantConditions")
    public SSDeepDiscoveryQueryLogic() {
        super();
        if (this.discoveryDelegate == null) { // may be set by super constructor
            this.discoveryDelegate = new DiscoveryLogic();
        }
    }

    public SSDeepDiscoveryQueryLogic(SSDeepDiscoveryQueryLogic other) {
        super(other);
        this.discoveryDelegate = (DiscoveryLogic) other.discoveryDelegate.clone();
    }

    @Override
    public QueryLogicTransformer getTransformer(final Query settings) {
        final DiscoveryTransformer discoveryTransformer = (DiscoveryTransformer) discoveryDelegate.getTransformer(settings);
        QueryLogicTransformer<DiscoveredSSDeep,EventBase> ssdeepTransformer = new AbstractQueryLogicTransformer<>() {
            @Override
            public BaseQueryResponse createResponse(List<Object> resultList) {
                return discoveryTransformer.createResponse(resultList);
            }

            @Override
            public EventBase transform(DiscoveredSSDeep discoveredSSDeep) {
                EventBase eventBase = discoveryTransformer.transform(discoveredSSDeep.getDiscoveredThing());
                ResponseObjectFactory responseObjectFactory = discoveryDelegate.getResponseObjectFactory();
                ScoredSSDeepPair scoredSSDeepPair = discoveredSSDeep.getScoredSSDeepPair();
                if (scoredSSDeepPair != null) {
                    List<FieldBase<?>> originalFields = eventBase.getFields();
                    Optional<FieldBase<?>> valueFieldOptional = originalFields.stream().filter(field -> "VALUE".equals(field.getName())).findFirst();

                    if (valueFieldOptional.isEmpty()) {
                        throw new IllegalStateException("Could not find value field in event");
                    }

                    FieldBase<?> valueField = valueFieldOptional.get();

                    // Handles the case where the DiscoveryQuery returns a down-cased ssdeep. To do this, we remove the
                    // original VALUE field from the discovery result and create a new one from the scoredSSDeepPair
                    // which was returned from the original similarity query. This also updates the list stored in the
                    // event with the filtered version.
                    final List<FieldBase<?>> newFields = originalFields.stream().filter(field -> !"VALUE".equals(field.getName())).collect(Collectors.toList());
                    eventBase.setFields(newFields);

                    {
                        FieldBase<?> field = responseObjectFactory.getField();
                        field.setName("QUERY");
                        field.setMarkings(valueField.getMarkings());
                        field.setColumnVisibility(valueField.getColumnVisibility());
                        field.setTimestamp(valueField.getTimestamp());
                        field.setValue(scoredSSDeepPair.getQueryHash().toString());
                        newFields.add(field);
                    }

                    {
                        // add a new value field that preserves the case of the original matched ssdeep.
                        FieldBase<?> field = responseObjectFactory.getField();
                        field.setName("VALUE");
                        field.setMarkings(valueField.getMarkings());
                        field.setColumnVisibility(valueField.getColumnVisibility());
                        field.setTimestamp(valueField.getTimestamp());
                        field.setValue(scoredSSDeepPair.getMatchingHash().toString());
                        newFields.add(field);
                    }

                    {
                        FieldBase<?> field = responseObjectFactory.getField();
                        field.setName("WEIGHTED_SCORE");
                        field.setMarkings(valueField.getMarkings());
                        field.setColumnVisibility(valueField.getColumnVisibility());
                        field.setTimestamp(valueField.getTimestamp());
                        field.setValue(scoredSSDeepPair.getWeightedScore());
                        newFields.add(field);
                    }

                    {
                        FieldBase<?> field = responseObjectFactory.getField();
                        field.setName("OVERLAP_SCORE");
                        field.setMarkings(valueField.getMarkings());
                        field.setColumnVisibility(valueField.getColumnVisibility());
                        field.setTimestamp(valueField.getTimestamp());
                        field.setValue(scoredSSDeepPair.getOverlapScore());
                        newFields.add(field);
                    }

                    {
                        FieldBase field = responseObjectFactory.getField();
                        field.setName("OVERLAP_SSDEEP_NGRAMS");
                        field.setMarkings(valueField.getMarkings());
                        field.setColumnVisibility(valueField.getColumnVisibility());
                        field.setTimestamp(valueField.getTimestamp());
                        field.setValue(scoredSSDeepPair.getOverlapsAsString());
                        newFields.add(field);
                    }
                }
                return eventBase;
            }
        };
        return ssdeepTransformer;

    }

    @Override
    public TransformIterator getTransformIterator(Query settings) {
        return new DatawaveTransformIterator(this.iterator(), this.getTransformer(settings));
    }

    /**
     * Return an iterator over the logic's results. This method must return an Iterator with the generic type compatible with generic type on the superclass
     * BaseQueryLogic&lt;DiscoveredSSDeep&gt;, but the actual DiscoveredSSDeep with a complete scoredSSDeepPair is generated by the enrichDiscoveredSSDeep()
     * method in FullSSDeepDiscoveryChainStrategy.
     */
    @Override
    public Iterator<DiscoveredSSDeep> iterator() {
        return new TransformIterator<>(discoveryDelegate.iterator(), discoveredThing -> new DiscoveredSSDeep(null, discoveredThing));
    }

    // All delegate methods past this point //

    public void setTableName(String tableName) {
        discoveryDelegate.setTableName(tableName);
    }

    public void setIndexTableName(String tableName) {
        discoveryDelegate.setIndexTableName(tableName);
    }

    public void setReverseIndexTableName(String tableName) {
        discoveryDelegate.setReverseIndexTableName(tableName);
    }

    public void setModelTableName(String tableName) {
        discoveryDelegate.setModelTableName(tableName);
    }

    public void setModelName(String modelName) {
        discoveryDelegate.setModelName(modelName);
    }

    public void setQueryModel(QueryModel model) {
        discoveryDelegate.setQueryModel(model);
    }

    public String getModelName() {
        return discoveryDelegate.getModelName();
    }

    public void setMetadataHelperFactory(MetadataHelperFactory metadataHelperFactory) {
        discoveryDelegate.setMetadataHelperFactory(metadataHelperFactory);
    }

    public void setResponseObjectFactory(ResponseObjectFactory responseObjectFactory) {
        discoveryDelegate.setResponseObjectFactory(responseObjectFactory);
    }

    public void setMarkingFunctions(MarkingFunctions markingFunctions) {
        discoveryDelegate.setMarkingFunctions(markingFunctions);
    }

    @Override
    public GenericQueryConfiguration initialize(AccumuloClient client, Query settings, Set<Authorizations> runtimeQueryAuthorizations) throws Exception {
        return discoveryDelegate.initialize(client, settings, runtimeQueryAuthorizations);
    }

    @Override
    public void setupQuery(GenericQueryConfiguration configuration) throws Exception {
        discoveryDelegate.setupQuery(configuration);
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return new SSDeepDiscoveryQueryLogic(this);
    }

    @Override
    public AccumuloConnectionFactory.Priority getConnectionPriority() {
        return discoveryDelegate.getConnectionPriority();
    }

    @Override
    public Set<String> getOptionalQueryParameters() {
        return discoveryDelegate.getOptionalQueryParameters();
    }

    public void setFullTableScanEnabled(boolean fullTableScanEnabled) {
        discoveryDelegate.setFullTableScanEnabled(fullTableScanEnabled);
    }

    public void setAllowLeadingWildcard(boolean allowLeadingWildcard) {
        discoveryDelegate.setAllowLeadingWildcard(allowLeadingWildcard);
    }

    @Override
    public Set<String> getRequiredQueryParameters() {
        return discoveryDelegate.getRequiredQueryParameters();
    }

    @Override
    public Set<String> getExampleQueries() {
        return discoveryDelegate.getExampleQueries();
    }

    @Override
    public GenericQueryConfiguration getConfig() {
        if (discoveryDelegate == null) {
            discoveryDelegate = new DiscoveryLogic();
        }
        return discoveryDelegate.getConfig();
    }

    @Override
    public String getPlan(AccumuloClient client, Query settings, Set<Authorizations> runtimeQueryAuthorizations, boolean expandFields, boolean expandValues)
                    throws Exception {
        return discoveryDelegate.getPlan(client, settings, runtimeQueryAuthorizations, expandFields, expandValues);
    }

    @Override
    public MarkingFunctions getMarkingFunctions() {
        return discoveryDelegate.getMarkingFunctions();
    }

    @Override
    public ResponseObjectFactory getResponseObjectFactory() {
        return discoveryDelegate.getResponseObjectFactory();
    }

    @Override
    public Principal getPrincipal() {
        return discoveryDelegate.getPrincipal();
    }

    @Override
    public void setPrincipal(Principal principal) {
        discoveryDelegate.setPrincipal(principal);
    }

    @Override
    public String getTableName() {
        return discoveryDelegate.getTableName();
    }

    @Override
    public long getMaxResults() {
        return discoveryDelegate.getMaxResults();
    }

    @Override
    public long getMaxWork() {
        return discoveryDelegate.getMaxWork();
    }

    @Override
    public void setMaxResults(long maxResults) {
        discoveryDelegate.setMaxResults(maxResults);
    }

    @Override
    public void setMaxWork(long maxWork) {
        discoveryDelegate.setMaxWork(maxWork);
    }

    @Override
    public int getMaxPageSize() {
        return discoveryDelegate.getMaxPageSize();
    }

    @Override
    public void setMaxPageSize(int maxPageSize) {
        discoveryDelegate.setMaxPageSize(maxPageSize);
    }

    @Override
    public long getPageByteTrigger() {
        return discoveryDelegate.getPageByteTrigger();
    }

    @Override
    public void setPageByteTrigger(long pageByteTrigger) {
        discoveryDelegate.setPageByteTrigger(pageByteTrigger);
    }

    @Override
    public int getBaseIteratorPriority() {
        return discoveryDelegate.getBaseIteratorPriority();
    }

    @Override
    public void setBaseIteratorPriority(int baseIteratorPriority) {
        discoveryDelegate.setBaseIteratorPriority(baseIteratorPriority);
    }

    @Override
    public String getLogicName() {
        return discoveryDelegate.getLogicName();
    }

    @Override
    public void setLogicName(String logicName) {
        discoveryDelegate.setLogicName(logicName);
    }

    @Override
    public boolean getBypassAccumulo() {
        return discoveryDelegate.getBypassAccumulo();
    }

    @Override
    public void setBypassAccumulo(boolean bypassAccumulo) {
        discoveryDelegate.setBypassAccumulo(bypassAccumulo);
    }

    @Override
    public String getAccumuloPassword() {
        return discoveryDelegate.getAccumuloPassword();
    }

    @Override
    public void setAccumuloPassword(String accumuloPassword) {
        discoveryDelegate.setAccumuloPassword(accumuloPassword);
    }

    @Override
    public Auditor.AuditType getAuditType(Query query) {
        return discoveryDelegate.getAuditType(query);
    }

    @Override
    public Auditor.AuditType getAuditType() {
        return discoveryDelegate.getAuditType();
    }

    @Override
    public void setAuditType(Auditor.AuditType auditType) {
        discoveryDelegate.setAuditType(auditType);
    }

    @Override
    public void setLogicDescription(String logicDescription) {
        discoveryDelegate.setLogicDescription(logicDescription);
    }

    @Override
    public String getLogicDescription() {
        return discoveryDelegate.getLogicDescription();
    }

    @Override
    public boolean getCollectQueryMetrics() {
        return discoveryDelegate.getCollectQueryMetrics();
    }

    @Override
    public void setCollectQueryMetrics(boolean collectQueryMetrics) {
        discoveryDelegate.setCollectQueryMetrics(collectQueryMetrics);
    }

    @Override
    public RoleManager getRoleManager() {
        return discoveryDelegate.getRoleManager();
    }

    @Override
    public void setRoleManager(RoleManager roleManager) {
        discoveryDelegate.setRoleManager(roleManager);
    }

    @Override
    public String getConnPoolName() {
        return discoveryDelegate.getConnPoolName();
    }

    @Override
    public void setConnPoolName(String connPoolName) {
        discoveryDelegate.setConnPoolName(connPoolName);
    }

    @Override
    public boolean canRunQuery() {
        return discoveryDelegate.canRunQuery();
    }

    @Override
    public boolean canRunQuery(Principal principal) {
        return discoveryDelegate.canRunQuery(principal);
    }

    @Override
    public List<String> getSelectors(Query settings) throws IllegalArgumentException {
        return discoveryDelegate.getSelectors(settings);
    }

    @Override
    public void setSelectorExtractor(SelectorExtractor selectorExtractor) {
        discoveryDelegate.setSelectorExtractor(selectorExtractor);
    }

    @Override
    public SelectorExtractor getSelectorExtractor() {
        return discoveryDelegate.getSelectorExtractor();
    }

    @Override
    public Set<String> getAuthorizedDNs() {
        return discoveryDelegate.getAuthorizedDNs();
    }

    @Override
    public void setAuthorizedDNs(Set<String> authorizedDNs) {
        discoveryDelegate.setAuthorizedDNs(authorizedDNs);
    }

    @Override
    public void setDnResultLimits(Map<String,Long> dnResultLimits) {
        discoveryDelegate.setDnResultLimits(dnResultLimits);
    }

    @Override
    public Map<String,Long> getDnResultLimits() {
        return discoveryDelegate.getDnResultLimits();
    }

    @Override
    public void setSystemFromResultLimits(Map<String,Long> systemFromLimits) {
        discoveryDelegate.setSystemFromResultLimits(systemFromLimits);
    }

    @Override
    public Map<String,Long> getSystemFromResultLimits() {
        return discoveryDelegate.getSystemFromResultLimits();
    }

    @Override
    public void setPageProcessingStartTime(long pageProcessingStartTime) {
        discoveryDelegate.setPageProcessingStartTime(pageProcessingStartTime);
    }

    @Override
    public boolean isLongRunningQuery() {
        return discoveryDelegate.isLongRunningQuery();
    }

    @Override
    public ResponseEnricherBuilder getResponseEnricherBuilder() {
        return discoveryDelegate.getResponseEnricherBuilder();
    }

    @Override
    public void setResponseEnricherBuilder(ResponseEnricherBuilder responseEnricherBuilder) {
        discoveryDelegate.setResponseEnricherBuilder(responseEnricherBuilder);
    }

    @Override
    public UserOperations getUserOperations() {
        return discoveryDelegate.getUserOperations();
    }

    @Override
    public String getResponseClass(Query query) throws QueryException {
        return discoveryDelegate.getResponseClass(query);
    }

    @Override
    public boolean containsDNWithAccess(Collection<String> dns) {
        return discoveryDelegate.containsDNWithAccess(dns);
    }

    @Override
    public long getResultLimit(Query settings) {
        return discoveryDelegate.getResultLimit(settings);
    }

}
