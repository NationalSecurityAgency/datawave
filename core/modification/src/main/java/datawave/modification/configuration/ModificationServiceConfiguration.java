package datawave.modification.configuration;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.modification.query.ModificationQueryService;
import datawave.security.authorization.ProxiedUserDetails;
import datawave.webservice.modification.ModificationRequestBase;

public abstract class ModificationServiceConfiguration {

    protected boolean requiresAudit = true;

    protected String description = null;
    protected List<String> authorizedRoles = null;
    protected ModificationQueryService.ModificationQueryServiceFactory queryServiceFactory = null;
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
     * @return ModificationQueryService
     */
    public ModificationQueryService getQueryService(ProxiedUserDetails userDetails) {
        return queryServiceFactory.createService(userDetails);
    }

    public ModificationQueryService.ModificationQueryServiceFactory getQueryServiceFactory() {
        return queryServiceFactory;
    }

    public void setQueryServiceFactory(ModificationQueryService.ModificationQueryServiceFactory queryServiceFactory) {
        this.queryServiceFactory = queryServiceFactory;
    }

    /**
     * The actual object type required for this service
     *
     * @return the request class
     */
    public abstract Class<? extends ModificationRequestBase> getRequestClass();

    /**
     *
     * @param client
     *            Accumulo Connector
     * @param request
     *            the modification request to process
     * @param mutableFieldList
     *            map of datatype to set of fields that are mutable
     * @param userAuths
     *            authorizations of user making the call
     * @param userDetails
     *            user details
     * @throws Exception
     *             if there is an issue
     */
    public abstract void process(AccumuloClient client, ModificationRequestBase request, Map<String,Set<String>> mutableFieldList,
                    Set<Authorizations> userAuths, ProxiedUserDetails userDetails) throws Exception;

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
