package datawave.configuration;

import javax.annotation.security.DeclareRoles;
import javax.annotation.security.PermitAll;
import javax.annotation.security.RolesAllowed;
import javax.annotation.security.RunAs;
import javax.ejb.Singleton;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.enterprise.event.Event;
import javax.enterprise.inject.Any;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import com.codahale.metrics.annotation.Timed;
import datawave.webservice.result.GenericResponse;
import org.apache.deltaspike.core.api.jmx.JmxManaged;
import org.apache.deltaspike.core.api.jmx.MBean;

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
