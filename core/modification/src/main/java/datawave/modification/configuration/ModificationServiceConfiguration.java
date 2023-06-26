package datawave.modification.configuration;

import datawave.modification.query.ModificationQueryService;
import datawave.security.authorization.DatawaveUser;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.webservice.modification.ModificationRequestBase;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    public ModificationQueryService getQueryService(Collection<? extends DatawaveUser> proxiedUsers) {
        return queryServiceFactory.createService(proxiedUsers);
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
     * @param proxiedUsers
     *            user proxy chain
     * @throws Exception
     *             if there is an issue
     */
    public abstract void process(AccumuloClient client, ModificationRequestBase request, Map<String,Set<String>> mutableFieldList,
                    Set<Authorizations> userAuths, Collection<? extends DatawaveUser> proxiedUsers) throws Exception;

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
