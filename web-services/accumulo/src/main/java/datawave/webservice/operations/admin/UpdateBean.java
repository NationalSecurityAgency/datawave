package datawave.webservice.operations.admin;

import datawave.webservice.operations.remote.RemoteAdminService;
import datawave.webservice.request.UpdateRequest;
import datawave.webservice.response.UpdateResponse;
import datawave.webservice.response.ValidateVisibilityResponse;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.security.DeclareRoles;
import jakarta.annotation.security.RolesAllowed;
import jakarta.ejb.LocalBean;
import jakarta.ejb.Stateless;
import jakarta.ejb.TransactionAttribute;
import jakarta.ejb.TransactionAttributeType;
import jakarta.ejb.TransactionManagement;
import jakarta.ejb.TransactionManagementType;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.FormParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;

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
