package datawave.configuration;

import org.apache.deltaspike.core.api.jmx.JmxManaged;
import org.apache.deltaspike.core.api.jmx.MBean;

import com.codahale.metrics.annotation.Timed;

import datawave.webservice.result.GenericResponse;
import jakarta.annotation.security.DeclareRoles;
import jakarta.annotation.security.PermitAll;
import jakarta.annotation.security.RolesAllowed;
import jakarta.annotation.security.RunAs;
import jakarta.ejb.Singleton;
import jakarta.ejb.TransactionAttribute;
import jakarta.ejb.TransactionAttributeType;
import jakarta.enterprise.event.Event;
import jakarta.enterprise.inject.Any;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;

@MBean
@Singleton
@Path("/Common/Configuration")
// ensure we can invoke the event receiver on any protected EJBs that are listening
@RunAs("InternalUser")
@RolesAllowed({"InternalUser", "Administrator", "JBossAdministrator"})
@DeclareRoles({"InternalUser", "Administrator", "JBossAdministrator"})
// transactions not supported directly by this bean
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
public class ConfigurationBean {
    @Inject
    @Any
    private Event<ConfigurationEvent> configurationEvent;
    @Inject
    @Any
    private Event<RefreshEvent> refreshEvent;
    @Inject
    @Any
    private Event<RefreshLifecycle> refreshLifecycleEvent;

    @PermitAll
    @JmxManaged
    public void refreshInternal() {
        refreshLifecycleEvent.fire(RefreshLifecycle.INITIATED);

        // Tell spring configurations to reload
        configurationEvent.fire(new ConfigurationEvent());
        // Now refresh the refreshable context to ensure beans that need to be are recreated.
        refreshEvent.fire(new RefreshEvent());

        refreshLifecycleEvent.fire(RefreshLifecycle.COMPLETE);
    }

    /**
     * Causes reloadable configuration to be re-read from disk. If a spring config file on disk is edited, or a system property is changed, call this endpoint
     * to have all classes that are able to refresh themselves using the updated configuration.
     *
     * @return endpoint response
     */
    @GET
    @Path("/refresh")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @Timed(name = "dw.config.refresh", absolute = true)
    public GenericResponse<String> refresh() {
        GenericResponse<String> response = new GenericResponse<>();
        refreshInternal();
        response.setResult("Configuration refreshed.");
        return response;
    }
}
