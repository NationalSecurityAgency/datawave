package datawave.microservice.accumulo.admin;

import datawave.webservice.request.UpdateRequest;
import datawave.webservice.response.ListTablesResponse;
import datawave.webservice.response.ListUserAuthorizationsResponse;
import datawave.webservice.response.ListUserPermissionsResponse;
import datawave.webservice.response.ListUsersResponse;
import datawave.webservice.response.UpdateResponse;
import datawave.webservice.response.ValidateVisibilityResponse;
import datawave.webservice.result.VoidResponse;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.security.RolesAllowed;

@RestController
@RolesAllowed({"InternalUser", "Administrator"})
@RequestMapping(path = "/v1", produces = {MediaType.APPLICATION_XML_VALUE, MediaType.TEXT_XML_VALUE, MediaType.APPLICATION_JSON_VALUE})
@ConditionalOnProperty(name = "accumulo.admin.enabled", havingValue = "true", matchIfMissing = true)
public class AdminController {
    
    private final AdminService adminService;
    
    @Autowired
    public AdminController(AdminService adminService) {
        this.adminService = adminService;
    }
    
    //@formatter:off
    @ApiOperation(value = "Grants the Accumulo permission to the specified user")
    @RequestMapping(path = "/GrantSystemPermission/{userName}/{permission}", method = {RequestMethod.POST})
    public VoidResponse grantSystemPermission(
            @ApiParam("The Accumulo user") @PathVariable String userName,
            @ApiParam("Permission to be granted") @PathVariable String permission) {
        return this.adminService.grantSystemPermission(userName, permission);
        //@formatter:on
    }
    
    @ApiOperation(value = "Creates the specified table in Accumulo")
    @RequestMapping(path = "/CreateTable/{tableName}", method = {RequestMethod.POST})
    public VoidResponse createTable(@ApiParam("Accumulo table name to create") @PathVariable String tableName) {
        return this.adminService.createTable(tableName);
    }
    
    @ApiOperation(value = "Flushes the memory buffer of the specified table to disk (minor compaction)")
    @RequestMapping(path = "/FlushTable/{tableName}", method = {RequestMethod.POST})
    public VoidResponse flushTable(@ApiParam("Accumulo table to flush") @PathVariable String tableName) {
        return this.adminService.flushTable(tableName);
    }
    
    @ApiOperation(value = "Sets the property on the specified Accumulo table")
    @RequestMapping(path = "/SetTableProperty/{tableName}/{propertyName}/{propertyValue}", method = {RequestMethod.POST})
    //@formatter:off
    public VoidResponse setTableProperty(
            @ApiParam("Accumulo table name") @PathVariable String tableName,
            @ApiParam("Property to set") @PathVariable String propertyName,
            @ApiParam("Property value to set") @PathVariable String propertyValue) {
        //@formatter:on
        return this.adminService.setTableProperty(tableName, propertyName, propertyValue);
    }
    
    @ApiOperation(value = "Removes the property from the specified Accumulo table")
    @RequestMapping(path = "/RemoveTableProperty/{tableName}/{propertyName}", method = {RequestMethod.POST})
    //@formatter:off
    public VoidResponse removeTableProperty(
            @ApiParam("Accumulo table name") @PathVariable String tableName,
            @ApiParam("Property to remove") @PathVariable String propertyName) {
        //@formatter:on
        return this.adminService.removeTableProperty(tableName, propertyName);
    }
    
    @ApiOperation(value = "Writes Accumulo mutations prescribed by the given request")
    @RequestMapping(path = "/Update", method = {RequestMethod.PUT}, consumes = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public UpdateResponse doUpdate(@ApiParam("UpdateRequest containing mutations to write to Accumulo") @RequestBody UpdateRequest request) {
        return this.adminService.updateAccumulo(request);
    }
    
    @ApiOperation(value = "Validate that the accumulo user can see this visibility, and return the printable strings that correspond with this visibility")
    @RequestMapping(path = "/ValidateVisibilities", method = {RequestMethod.POST})
    public ValidateVisibilityResponse validateVisibilities(@ApiParam("Visibility strings to validate") @RequestBody String[] visibilityArray) {
        return this.adminService.validateVisibilities(visibilityArray);
    }
    
    @ApiOperation(value = "Returns the list of Accumulo table names")
    @RequestMapping(path = "/ListTables", method = {RequestMethod.GET})
    public ListTablesResponse listTables() {
        return this.adminService.listTables();
    }
    
    @RequestMapping(path = "/ListUserAuthorizations/{userName}", method = {RequestMethod.GET})
    public ListUserAuthorizationsResponse listUserAuthorizations(@ApiParam("Accumulo user name") @PathVariable String userName) {
        return this.adminService.listUserAuthorizations(userName);
    }
    
    @RequestMapping(path = "/ListUserPermissions/{userName}", method = {RequestMethod.GET})
    public ListUserPermissionsResponse listUserPermissions(@ApiParam("Accumulo user name") @PathVariable String userName) {
        return this.adminService.listUserPermissions(userName);
    }
    
    @ApiOperation(value = "Returns the list of Accumulo users")
    @RequestMapping(path = "/ListUsers", method = {RequestMethod.GET})
    public ListUsersResponse listUsers() {
        return this.adminService.listUsers();
    }
}
