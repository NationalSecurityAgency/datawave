package datawave.webservice.operations.admin;

import datawave.webservice.operations.remote.RemoteAdminService;
import datawave.webservice.request.UpdateRequest;
import datawave.webservice.response.UpdateResponse;
import datawave.webservice.response.ValidateVisibilityResponse;

import javax.annotation.PostConstruct;
import javax.annotation.security.DeclareRoles;
import javax.annotation.security.RolesAllowed;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.ejb.TransactionManagement;
import javax.ejb.TransactionManagementType;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

@Path("/Accumulo")
@RolesAllowed({"InternalUser", "Administrator"})
@DeclareRoles({"InternalUser", "Administrator"})
@LocalBean
@Stateless
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
@TransactionManagement(TransactionManagementType.BEAN)
public class UpdateBean {
    
    @Inject
    private RemoteAdminService remoteAdminService;
    
    @PostConstruct
    public void init() {}
    
    @Path("/Update")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml"})
    @Consumes("application/xml")
    @PUT
    public UpdateResponse doUpdate(UpdateRequest request) {
        
        return remoteAdminService.update(request);
    }
    
    @Path("ValidateVisibilities")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml"})
    @POST
    public ValidateVisibilityResponse validateVisibilities(@FormParam("visibility") String[] visibilityArray) {
        
        return remoteAdminService.validateVisibilities(visibilityArray);
    }
}
