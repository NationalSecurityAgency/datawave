package datawave.microservice.accumulo.admin;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.MediaType;
import org.springframework.security.access.annotation.Secured;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import datawave.webservice.request.UpdateRequest;
import datawave.webservice.response.ListTablesResponse;
import datawave.webservice.response.ListUserAuthorizationsResponse;
import datawave.webservice.response.ListUserPermissionsResponse;
import datawave.webservice.response.ListUsersResponse;
import datawave.webservice.response.UpdateResponse;
import datawave.webservice.response.ValidateVisibilityResponse;
import datawave.webservice.result.VoidResponse;
import io.swagger.v3.oas.annotations.ExternalDocumentation;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Admin Controller /v1", description = "DataWave Admin Operations",
                externalDocs = @ExternalDocumentation(description = "Accumulo Service Documentation",
                                url = "https://github.com/NationalSecurityAgency/datawave-accumulo-service"))
@RestController
@Secured({"InternalUser", "Administrator"})
@RequestMapping(path = "/v1", produces = {MediaType.APPLICATION_XML_VALUE, MediaType.TEXT_XML_VALUE, MediaType.APPLICATION_JSON_VALUE})
@ConditionalOnProperty(name = "accumulo.admin.enabled", havingValue = "true", matchIfMissing = true)
public class AdminController {
    
    private final AdminService adminService;
    
    @Autowired
    public AdminController(AdminService adminService) {
        this.adminService = adminService;
    }
    
    //@formatter:off
    @Operation(summary = "Grants the Accumulo permission to the specified user")
    @RequestMapping(path = "/admin/grantSystemPermission/{userName}/{permission}", method = {RequestMethod.POST})
    public VoidResponse grantSystemPermission(
            @Parameter(description = "The Accumulo user") @PathVariable String userName,
            @Parameter(description = "Permission to be granted") @PathVariable String permission) {
        return this.adminService.grantSystemPermission(userName, permission);
        //@formatter:on
    }
    
    //@formatter:off
    @Operation(summary = "Revokes the Accumulo permission from the specified user")
    @RequestMapping(path = "/admin/revokeSystemPermission/{userName}/{permission}", method = {RequestMethod.POST})
    public VoidResponse revokeSystemPermission(
            @Parameter(description = "The Accumulo user") @PathVariable String userName,
            @Parameter(description = "Permission to be revoked") @PathVariable String permission) {
        return this.adminService.revokeSystemPermission(userName, permission);
        //@formatter:on
    }
    
    //@formatter:off
    @Operation(summary = "Grants the table permission to the specified Accumulo user")
    @RequestMapping(path = "/admin/grantTablePermission/{userName}/{tableName}/{permission}", method = {RequestMethod.POST})
    public VoidResponse grantTablePermission(
            @Parameter(description = "The Accumulo user") @PathVariable String userName,
            @Parameter(description = "The Accumulo table") @PathVariable String tableName,
            @Parameter(description = "Permission to be granted") @PathVariable String permission) {
        return this.adminService.grantTablePermission(userName, tableName, permission);
        //@formatter:on
    }
    
    //@formatter:off
    @Operation(summary = "Revokes the table permission from the specified Accumulo user")
    @RequestMapping(path = "/admin/revokeTablePermission/{userName}/{tableName}/{permission}", method = {RequestMethod.POST})
    public VoidResponse revokeTablePermission(
            @Parameter(description = "The Accumulo user") @PathVariable String userName,
            @Parameter(description = "The Accumulo table") @PathVariable String tableName,
            @Parameter(description = "Permission to be revoked") @PathVariable String permission) {
        return this.adminService.revokeTablePermission(userName, tableName, permission);
        //@formatter:on
    }
    
    @Operation(summary = "Creates the specified table in Accumulo")
    @RequestMapping(path = "/admin/createTable/{tableName}", method = {RequestMethod.POST})
    public VoidResponse createTable(@Parameter(description = "Accumulo table name to create") @PathVariable String tableName) {
        return this.adminService.createTable(tableName);
    }
    
    @Operation(summary = "Flushes the memory buffer of the specified table to disk (minor compaction)")
    @RequestMapping(path = "/admin/flushTable/{tableName}", method = {RequestMethod.POST})
    public VoidResponse flushTable(@Parameter(description = "Accumulo table to flush") @PathVariable String tableName) {
        return this.adminService.flushTable(tableName);
    }
    
    @Operation(summary = "Sets the property on the specified Accumulo table")
    @RequestMapping(path = "/admin/setTableProperty/{tableName}/{propertyName}/{propertyValue}", method = {RequestMethod.POST})
    //@formatter:off
    public VoidResponse setTableProperty(
            @Parameter(description = "Accumulo table name") @PathVariable String tableName,
            @Parameter(description = "Property to set") @PathVariable String propertyName,
            @Parameter(description = "Property value to set") @PathVariable String propertyValue) {
        //@formatter:on
        return this.adminService.setTableProperty(tableName, propertyName, propertyValue);
    }
    
    @Operation(summary = "Removes the property from the specified Accumulo table")
    @RequestMapping(path = "/admin/removeTableProperty/{tableName}/{propertyName}", method = {RequestMethod.POST})
    //@formatter:off
    public VoidResponse removeTableProperty(
            @Parameter(description = "Accumulo table name") @PathVariable String tableName,
            @Parameter(description = "Property to remove") @PathVariable String propertyName) {
        //@formatter:on
        return this.adminService.removeTableProperty(tableName, propertyName);
    }
    
    @Operation(summary = "Writes Accumulo mutations prescribed by the given request")
    @RequestMapping(path = "/admin/update", method = {RequestMethod.PUT}, consumes = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public UpdateResponse doUpdate(@Parameter(description = "UpdateRequest containing mutations to write to Accumulo") @RequestBody UpdateRequest request) {
        return this.adminService.updateAccumulo(request);
    }
    
    @Operation(summary = "Validate that the accumulo user can see this visibility, and return the printable strings that correspond with this visibility")
    @RequestMapping(path = "/admin/validateVisibilities", method = {RequestMethod.POST})
    public ValidateVisibilityResponse validateVisibilities(
                    @Parameter(description = "Visibility strings to validate") @RequestParam("visibility") String[] visibilityArray) {
        return this.adminService.validateVisibilities(visibilityArray);
    }
    
    @Operation(summary = "Returns the list of Accumulo table names")
    @RequestMapping(path = "/admin/listTables", method = {RequestMethod.GET})
    public ListTablesResponse listTables() {
        return this.adminService.listTables();
    }
    
    @Operation(summary = "Returns the specified Accumulo user's authorizations")
    @RequestMapping(path = "/admin/listUserAuthorizations/{userName}", method = {RequestMethod.GET})
    public ListUserAuthorizationsResponse listUserAuthorizations(@Parameter(description = "Accumulo user name") @PathVariable String userName) {
        return this.adminService.listUserAuthorizations(userName);
    }
    
    @Operation(summary = "Returns the specified Accumulo user's permissions")
    @RequestMapping(path = "/admin/listUserPermissions/{userName}", method = {RequestMethod.GET})
    public ListUserPermissionsResponse listUserPermissions(@Parameter(description = "Accumulo user name") @PathVariable String userName) {
        return this.adminService.listUserPermissions(userName);
    }
    
    @Operation(summary = "Returns the list of Accumulo users")
    @RequestMapping(path = "/admin/listUsers", method = {RequestMethod.GET})
    public ListUsersResponse listUsers() {
        return this.adminService.listUsers();
    }
}
