package datawave.webservice.operations.user;

import datawave.webservice.operations.remote.RemoteAdminService;
import datawave.webservice.response.ListUserPermissionsResponse;

import javax.annotation.security.DeclareRoles;
import javax.annotation.security.RolesAllowed;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.ejb.TransactionManagement;
import javax.ejb.TransactionManagementType;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

@Path("/Accumulo")
@RolesAllowed({"InternalUser", "Administrator"})
@DeclareRoles({"InternalUser", "Administrator"})
@LocalBean
@Stateless
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
@TransactionManagement(TransactionManagementType.BEAN)
public class ListUserPermissionsBean {
    
    @Inject
    private RemoteAdminService remoteAdminService;
    
    @Path("/ListUserPermissions/{userName}")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml"})
    @GET
    public ListUserPermissionsResponse listUserPermissions(@PathParam("userName") String userName) {
        
        return remoteAdminService.listUserPermissions(userName);
    }
    
}
