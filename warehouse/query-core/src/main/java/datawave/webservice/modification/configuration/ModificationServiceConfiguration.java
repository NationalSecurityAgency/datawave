package datawave.webservice.modification.configuration;

import java.util.List;
import java.util.Map;
import java.util.Set;

import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.modification.ModificationRequestBase;
import datawave.webservice.query.runner.QueryExecutorBean;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;

public abstract class ModificationServiceConfiguration {
    
    protected boolean requiresAudit = true;
    
    protected String description = null;
    protected List<String> authorizedRoles = null;
    protected QueryExecutorBean queryService = null;
    protected List<String> securityMarkingExemptFields = null;
    
    public String getDescription() {
        return description;
    }
    
    public void setDescription(String description) {
        this.description = description;
    }
    
    public List<String> getAuthorizedRoles() {
        return authorizedRoles;
    }
    
    public void setAuthorizedRoles(List<String> authorizedRoles) {
        this.authorizedRoles = authorizedRoles;
    }
    
    public List<String> getSecurityMarkingExemptFields() {
        return securityMarkingExemptFields;
    }
    
    public void setSecurityMarkingExemptFields(List<String> securityMarkingExemptFields) {
        this.securityMarkingExemptFields = securityMarkingExemptFields;
    }
    
    /**
     * Handle to query service in case the modification service needs to run queries.
     * 
     * @return RemoteQueryExecutor
     */
    public QueryExecutorBean getQueryService() {
        return queryService;
    }
    
    public void setQueryService(QueryExecutorBean queryService) {
        this.queryService = queryService;
    }
    
    /**
     * The actual object type required for this service
     * 
     * @return the request class
     */
    public abstract Class<? extends ModificationRequestBase> getRequestClass();
    
    /**
     * 
     * @param con
     *            Accumulo Connector
     * @param request
     *            the modification request to process
     * @param mutableFieldList
     *            map of datatype to set of fields that are mutable
     * @param userAuths
     *            authorizations of user making the call
     * @param user
     *            user identifier
     * @throws Exception
     *             if there is an issue
     */
    public abstract void process(Connector con, ModificationRequestBase request, Map<String,Set<String>> mutableFieldList, Set<Authorizations> userAuths,
                    String user) throws Exception;
    
    /**
     * 
     * @return priority from AccumuloConnectionFactory
     */
    public abstract AccumuloConnectionFactory.Priority getPriority();
    
    public boolean getRequiresAudit() {
        return requiresAudit;
    }
    
    public void setRequiresAudit(boolean requiresAudit) {
        this.requiresAudit = requiresAudit;
    }
    
}
