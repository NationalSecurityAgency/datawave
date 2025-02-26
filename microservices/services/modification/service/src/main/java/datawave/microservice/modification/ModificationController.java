package datawave.microservice.modification;

import static datawave.microservice.http.converter.protostuff.ProtostuffHttpMessageConverter.PROTOSTUFF_VALUE;

import java.util.List;

import org.springframework.http.MediaType;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.codahale.metrics.annotation.Timed;

import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.modification.ModificationService;
import datawave.webservice.modification.ModificationRequestBase;
import datawave.webservice.result.VoidResponse;
import datawave.webservice.results.modification.ModificationConfigurationResponse;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;

@RestController
@RequestMapping(path = "/v1", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE, MediaType.TEXT_XML_VALUE, PROTOSTUFF_VALUE,
        "text/x-yaml", "application/x-yaml", "text/yaml", "application/x-protobuf"})
public class ModificationController {
    
    private final ModificationService service;
    
    public ModificationController(ModificationService service) {
        this.service = service;
    }
    
    /**
     * Returns a list of the Modification service names and their configurations
     *
     * @return List&lt;kdatawave.webservice.results.modification.ModificationConfigurationResponse&gt;
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     */
    // @formatter:off
    @Operation(
            summary = "Return a list of the modification service configurations",
            description = "Returns a list of ModificationConfigurationResponse which contains the modification service configurations.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a list of ModificationConfigurationResponse containing service configurations",
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = ModificationConfigurationResponse.class))),
            @ApiResponse(
                    description = "if the user doesn't have access to this call",
                    responseCode = "401",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if there is an unknown error",
                    responseCode = "500",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class)))})
    // @formatter:on
    @GetMapping("/listConfigurations")
    @Timed(name = "dw.modification.list.configurations", absolute = true)
    public List<ModificationConfigurationResponse> listConfigurations() {
        return service.listConfigurations();
    }
    
    /**
     * Execute a Modification service with the given name and runtime parameters
     *
     * @param modificationServiceName
     *            Name of the modification service configuration
     * @param request
     *            object type specified in listConfigurations response.
     * @return datawave.webservice.result.VoidResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     * @HTTP 200 success
     * @HTTP 400 if jobName is invalid
     * @HTTP 401 if user does not have correct roles
     * @HTTP 500 error starting the job
     */
    // @formatter:off
    @Operation(
            summary = "Submit a modification service request",
            description = "Submit a modification service request.<br>" +
                          "")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a list of ModificationConfigurationResponse containing service configurations",
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = ModificationConfigurationResponse.class))),
            @ApiResponse(
                    description = "if the user doesn't have access to this call",
                    responseCode = "401",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if there is an unknown error",
                    responseCode = "500",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class)))})
    // @formatter:on
    @PutMapping(path = "/{modificationServiceName}/submit",
                    consumes = {MediaType.APPLICATION_XML_VALUE, MediaType.TEXT_XML_VALUE, MediaType.APPLICATION_JSON_VALUE})
    @Timed(name = "dw.modification.data.submit", absolute = true)
    public VoidResponse submit(@PathVariable String modificationServiceName, @RequestBody(required = true) ModificationRequestBase request,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) {
        return service.submit(currentUser, modificationServiceName, request);
    }
    
}
