package datawave.modification;

import static java.util.Map.Entry;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.modification.cache.ModificationCache;
import datawave.modification.configuration.ModificationConfiguration;
import datawave.modification.configuration.ModificationServiceConfiguration;
import datawave.modification.query.ModificationQueryService;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.ProxiedUserDetails;
import datawave.webservice.modification.ModificationRequestBase;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.exception.UnauthorizedQueryException;
import datawave.webservice.result.VoidResponse;
import datawave.webservice.results.modification.ModificationConfigurationResponse;

public class ModificationService {

    private static final Logger log = LoggerFactory.getLogger(ModificationService.class);

    private final AccumuloConnectionFactory connectionFactory;

    private final ModificationCache cache;

    private final ModificationQueryService.ModificationQueryServiceFactory queryServiceFactory;

    private final ModificationConfiguration modificationConfiguration;

    public ModificationService(ModificationConfiguration modificationConfiguration, ModificationCache cache, AccumuloConnectionFactory connectionFactory,
                    ModificationQueryService.ModificationQueryServiceFactory queryServiceFactory) {
        this.modificationConfiguration = modificationConfiguration;
        this.cache = cache;
        this.connectionFactory = connectionFactory;
        this.queryServiceFactory = queryServiceFactory;
    }

    /**
     * Returns a list of the Modification service names and their configurations
     *
     * @return datawave.webservice.results.modification.ModificationConfigurationResponse
     */
    public List<ModificationConfigurationResponse> listConfigurations() {
        List<ModificationConfigurationResponse> configs = new ArrayList<>();
        for (Entry<String,ModificationServiceConfiguration> entry : this.modificationConfiguration.getConfigurations().entrySet()) {
            ModificationConfigurationResponse r = new ModificationConfigurationResponse();
            r.setName(entry.getKey());
            r.setRequestClass(entry.getValue().getRequestClass().getName());
            r.setDescription(entry.getValue().getDescription());
            r.setAuthorizedRoles(entry.getValue().getAuthorizedRoles());
            configs.add(r);
        }
        return configs;
    }

    /**
     * Execute a Modification service with the given name and runtime parameters
     *
     * @param userDetails
     *            The proxied user list
     * @param modificationServiceName
     *            Name of the modification service configuration
     * @param request
     *            object type specified in listConfigurations response.
     * @return datawave.webservice.result.VoidResponse
     */
    public VoidResponse submit(ProxiedUserDetails userDetails, String modificationServiceName, ModificationRequestBase request) {
        VoidResponse response = new VoidResponse();

        // Find out who/what called this method
        DatawaveUser primaryUser = userDetails.getPrimaryUser();
        String userDn = primaryUser.getDn().subjectDN();
        Collection<String> proxyServers = userDetails.getProxiedUsers().stream().map(u -> u.getDn().subjectDN()).collect(Collectors.toList());
        Collection<String> userRoles = primaryUser.getRoles();
        Set<Authorizations> cbAuths = userDetails.getProxiedUsers().stream().map(u -> new Authorizations(u.getAuths().toArray(new String[0])))
                        .collect(Collectors.toSet());

        AccumuloClient client = null;
        AccumuloConnectionFactory.Priority priority;
        try {
            // Get the Modification Service from the configuration
            ModificationServiceConfiguration service = modificationConfiguration.getConfiguration(modificationServiceName);
            if (!request.getClass().equals(service.getRequestClass())) {
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_REQUEST_CLASS,
                                MessageFormat.format("Requires: {0} but got {1}", service.getRequestClass().getName(), request.getClass().getName()));
                throw new DatawaveModificationException(qe);
            }

            priority = service.getPriority();

            // Ensure that the user is in the list of authorized roles
            if (null != service.getAuthorizedRoles()) {
                boolean authorized = !Collections.disjoint(userRoles, service.getAuthorizedRoles());
                if (!authorized) {
                    // Then the user does not have any of the authorized roles
                    UnauthorizedQueryException qe = new UnauthorizedQueryException(DatawaveErrorCode.JOB_EXECUTION_UNAUTHORIZED,
                                    MessageFormat.format("Requires one of: {0}", service.getAuthorizedRoles()));
                    throw new DatawaveModificationException(qe);
                }
            }

            // Process the modification
            Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            client = connectionFactory.getClient(userDn, proxyServers, modificationConfiguration.getPoolName(), priority, trackingMap);
            service.setQueryServiceFactory(queryServiceFactory);
            log.info("Processing modification request from user={}: \n{}", userDetails.getShortName(), request);
            service.process(client, request, cache.getCachedMutableFieldList(), cbAuths, userDetails);
        } catch (DatawaveModificationException e) {
            throw e;
        } catch (Exception e) {
            QueryException qe = new QueryException(DatawaveErrorCode.MODIFICATION_ERROR, e);
            log.error("QueryException:", qe);
            throw new DatawaveModificationException(qe);
        } finally {
            if (null != client) {
                try {
                    connectionFactory.returnClient(client);
                } catch (Exception e) {
                    log.error("Error returning connection", e);
                }
            }
        }

        return response;
    }

}
