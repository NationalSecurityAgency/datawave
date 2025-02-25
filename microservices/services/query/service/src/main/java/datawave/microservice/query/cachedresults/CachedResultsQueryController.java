package datawave.microservice.query.cachedresults;

import static datawave.core.query.cachedresults.CachedResultsQueryParameters.CONDITIONS;
import static datawave.core.query.cachedresults.CachedResultsQueryParameters.FIELDS;
import static datawave.core.query.cachedresults.CachedResultsQueryParameters.GROUPING;
import static datawave.core.query.cachedresults.CachedResultsQueryParameters.ORDER;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.MediaType;
import org.springframework.security.access.annotation.Secured;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.codahale.metrics.annotation.Timed;

import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.CachedResultsDescribeResponse;
import datawave.webservice.result.CachedResultsResponse;
import datawave.webservice.result.GenericResponse;
import datawave.webservice.result.VoidResponse;
import io.swagger.v3.oas.annotations.ExternalDocumentation;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Cached Results Query Controller /v1", description = "DataWave Cached Results Query Management",
                externalDocs = @ExternalDocumentation(description = "Cached Results Query Documentation",
                                url = "https://github.com/NationalSecurityAgency/datawave-spring-boot-starter-cached-results"))
@RestController
@RequestMapping(path = "/v1/cachedresults", produces = MediaType.APPLICATION_JSON_VALUE)
@ConditionalOnProperty(name = "datawave.query.cached-results.enabled", havingValue = "true", matchIfMissing = true)
public class CachedResultsQueryController {
    
    private final CachedResultsQueryService cachedResultsQueryService;
    
    public CachedResultsQueryController(CachedResultsQueryService cachedResultsQueryService) {
        this.cachedResultsQueryService = cachedResultsQueryService;
    }
    
    // @see CachedResultsQueryService#load(String, String, DatawaveUserDetails)
    // @formatter:off
    @Operation(summary = "Loads a query into MySQL using the given defined query ID and parameters.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a generic response containing the view name.",
                    responseCode = "200")})
    // @formatter:on
    @RequestMapping(path = "{definedQueryId}/load", method = {RequestMethod.GET}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public GenericResponse<String> load(@Parameter(description = "The defined query ID") @PathVariable String definedQueryId,
                    @Parameter(description = "The user-defined alias") @RequestParam(required = false) String alias,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) throws QueryException {
        return cachedResultsQueryService.load(definedQueryId, alias, currentUser);
    }
    
    // @see CachedResultsQueryService#create(String, MultiValueMap, DatawaveUserDetails)
    // @formatter:off
    @Operation(
            summary = "Creates a MySQL query which will be run against the loaded results.",
            description = "Auditing is performed before the SQL query is started.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a cached results response containing information about the SQL query.",
                    responseCode = "200")})
    @Parameters({
            @Parameter(
                    name = FIELDS,
                    in = ParameterIn.QUERY,
                    description = "The fields to return",
                    schema = @Schema(implementation = String.class)),
            @Parameter(
                    name = CONDITIONS,
                    in = ParameterIn.QUERY,
                    description = "The conditions to apply",
                    schema = @Schema(implementation = String.class)),
            @Parameter(
                    name = GROUPING,
                    in = ParameterIn.QUERY,
                    description = "The fields to group by",
                    schema = @Schema(implementation = String.class)),
            @Parameter(
                    name = ORDER,
                    in = ParameterIn.QUERY,
                    description = "The fields to order by",
                    schema = @Schema(implementation = String.class))
    })
    // @formatter:on
    @RequestMapping(path = "{key}/create", method = {RequestMethod.POST},
                    produces = {"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
                            "application/x-protostuff"})
    @Timed(name = "dw.cachedr.create", absolute = true)
    public CachedResultsResponse create(@Parameter(description = "The defined query id, view name, or alias") @PathVariable String key,
                    @Parameter(hidden = true) @RequestParam MultiValueMap<String,String> parameters, @AuthenticationPrincipal DatawaveUserDetails currentUser)
                    throws QueryException {
        return cachedResultsQueryService.create(key, parameters, currentUser);
    }
    
    // @see CachedResultsQueryService#loadAndCreate(String, MultiValueMap, DatawaveUserDetails)
    // @formatter:off
    @Operation(
            summary = "Loads a query into MySQL using the given defined query ID and parameters, and creates a MySQL query which will be run against the loaded results",
            description = "Auditing is performed before the SQL query is started.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a cached results response containing information about the SQL query.",
                    responseCode = "200")})
    @Parameters({
            @Parameter(
                    name = FIELDS,
                    in = ParameterIn.QUERY,
                    description = "The fields to return",
                    schema = @Schema(implementation = String.class)),
            @Parameter(
                    name = CONDITIONS,
                    in = ParameterIn.QUERY,
                    description = "The conditions to apply",
                    schema = @Schema(implementation = String.class)),
            @Parameter(
                    name = GROUPING,
                    in = ParameterIn.QUERY,
                    description = "The fields to group by",
                    schema = @Schema(implementation = String.class)),
            @Parameter(
                    name = ORDER,
                    in = ParameterIn.QUERY,
                    description = "The fields to order by",
                    schema = @Schema(implementation = String.class))
    })
    // @formatter:on
    @RequestMapping(path = "{key}/loadAndCreate", method = {RequestMethod.POST},
                    produces = {"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
                            "application/x-protostuff"})
    @Timed(name = "dw.cachedr.loadAndCreate", absolute = true)
    public CachedResultsResponse loadAndCreate(@Parameter(description = "The defined query ID") @PathVariable String definedQueryId,
                    @Parameter(hidden = true) @RequestParam MultiValueMap<String,String> parameters, @AuthenticationPrincipal DatawaveUserDetails currentUser)
                    throws QueryException {
        return cachedResultsQueryService.loadAndCreate(definedQueryId, parameters, currentUser);
    }
    
    // @see CachedResultsQueryService#getRows(String, Integer, Integer, DatawaveUserDetails)
    // @formatter:off
    @Operation(
            summary = "Gets the requested rows from rowBegin to rowEnd for the specified query.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a base query response containing the requested rows.",
                    responseCode = "200")})
    // @formatter:on
    @RequestMapping(path = "{key}/getRows", method = {RequestMethod.GET},
                    produces = {"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
                            "application/x-protostuff"})
    @Timed(name = "dw.cachedr.getRows", absolute = true)
    public BaseQueryResponse getRows(@Parameter(description = "The defined query id, view name, or alias") @PathVariable("key") String key,
                    @RequestParam(defaultValue = "1") Integer rowBegin, @RequestParam Integer rowEnd, @AuthenticationPrincipal DatawaveUserDetails currentUser)
                    throws QueryException {
        return cachedResultsQueryService.getRows(key, rowBegin, rowEnd, currentUser);
    }
    
    // @see CachedResultsQueryService#status(String, DatawaveUserDetails)
    // @formatter:off
    @Operation(
            summary = "Gets the status for the specified query.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a generic response containing the query status.",
                    responseCode = "200")})
    // @formatter:on
    @RequestMapping(path = "{key}/status", method = {RequestMethod.GET},
                    produces = {"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
                            "application/x-protostuff"})
    @Timed(name = "dw.cachedr.status", absolute = true)
    public GenericResponse<String> status(@Parameter(description = "The defined query id, view name, or alias") @PathVariable("key") String key,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) throws QueryException {
        return cachedResultsQueryService.status(key, currentUser);
    }
    
    // @see CachedResultsQueryService#describe(String, DatawaveUserDetails)
    // @formatter:off
    @Operation(
            summary = "Gets the description for the specified query.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a cached results describe response containing the query description.",
                    responseCode = "200")})
    // @formatter:on
    @RequestMapping(path = "{key}/describe", method = {RequestMethod.GET}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public CachedResultsDescribeResponse describe(@Parameter(description = "The defined query id, view name, or alias") @PathVariable("key") String key,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) throws QueryException {
        return cachedResultsQueryService.describe(key, currentUser);
    }
    
    // @see CachedResultsQueryService#cancel(String, DatawaveUserDetails)
    // @formatter:off
    @Operation(summary = "Cancels the specified query.")
    // @formatter:on
    @RequestMapping(path = "{key}/cancel", method = {RequestMethod.PUT},
                    produces = {"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
                            "application/x-protostuff"})
    @Timed(name = "dw.cachedr.cancel", absolute = true)
    public VoidResponse cancel(@Parameter(description = "The defined query id, view name, or alias") @PathVariable("key") String key,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) throws QueryException {
        return cachedResultsQueryService.cancel(key, currentUser);
    }
    
    // @see CachedResultsQueryService#adminCancel(String, DatawaveUserDetails)
    // @formatter:off
    @Operation(summary = "Cancels the specified query using admin privileges.")
    // @formatter:on
    @Secured({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "{key}/adminCancel", method = {RequestMethod.PUT}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public VoidResponse adminCancel(@Parameter(description = "The defined query id, view name, or alias") @PathVariable("key") String key,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) throws QueryException {
        return cachedResultsQueryService.adminCancel(key, currentUser);
    }
    
    // @see CachedResultsQueryService#close(String, DatawaveUserDetails)
    // @formatter:off
    @Operation(summary = "Closes the specified query.")
    // @formatter:on
    @RequestMapping(path = "{key}/close", method = {RequestMethod.PUT},
                    produces = {"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
                            "application/x-protostuff"})
    @Timed(name = "dw.cachedr.close", absolute = true)
    public VoidResponse close(@Parameter(description = "The defined query id, view name, or alias") @PathVariable("key") String key,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) throws QueryException {
        return cachedResultsQueryService.close(key, currentUser);
    }
    
    // @see CachedResultsQueryService#adminClose(String, DatawaveUserDetails)
    // @formatter:off
    @Operation(summary = "Closes the specified query using admin privileges.")
    // @formatter:on
    @Secured({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "{key}/adminClose", method = {RequestMethod.PUT}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public VoidResponse adminClose(@Parameter(description = "The defined query id, view name, or alias") @PathVariable("key") String key,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) throws QueryException {
        return cachedResultsQueryService.adminClose(key, currentUser);
    }
    
    // @see CachedResultsQueryService#setAlias(String, String, DatawaveUserDetails)
    // @formatter:off
    @Operation(
            summary = "Sets the alias for the specified query.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a cached results response containing the updated alias.",
                    responseCode = "200")})
    // @formatter:on
    @RequestMapping(path = "{key}/setAlias", method = {RequestMethod.POST},
                    produces = {"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
                            "application/x-protostuff"})
    @Timed(name = "dw.cachedr.setAlias", absolute = true)
    public CachedResultsResponse setAlias(@Parameter(description = "The defined query id, view name, or alias") @PathVariable String key,
                    @RequestParam String alias, @AuthenticationPrincipal DatawaveUserDetails currentUser) throws QueryException {
        return cachedResultsQueryService.setAlias(key, alias, currentUser);
    }
    
    // @see CachedResultsQueryService#update(String, String, String, String, String, Integer, DatawaveUserDetails)
    // @formatter:off
    @Operation(
            summary = "Updates the cached results query.",
            description = "Auditing is performed if the SQL query changes.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a cached results response containing the updated alias.",
                    responseCode = "200")})
    // @formatter:on
    @RequestMapping(path = "{key}/update", method = {RequestMethod.POST},
                    produces = {"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
                            "application/x-protostuff"})
    @Timed(name = "dw.cachedr.update", absolute = true)
    public CachedResultsResponse update(@Parameter(description = "The defined query id, view name, or alias") @PathVariable String key,
                    @RequestParam(required = false) String fields, @RequestParam(required = false) String conditions,
                    @RequestParam(required = false) String grouping, @RequestParam(required = false) String order,
                    @RequestParam(required = false) Integer pagesize, @AuthenticationPrincipal DatawaveUserDetails currentUser) throws QueryException {
        return cachedResultsQueryService.update(key, fields, conditions, grouping, order, pagesize, currentUser);
    }
}
