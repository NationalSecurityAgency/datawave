package datawave.microservice.query;

import static datawave.core.query.logic.lookup.LookupQueryLogic.LOOKUP_KEY_VALUE_DELIMITER;
import static datawave.microservice.query.QueryParameters.QUERY_AUTHORIZATIONS;
import static datawave.microservice.query.QueryParameters.QUERY_BEGIN;
import static datawave.microservice.query.QueryParameters.QUERY_END;
import static datawave.microservice.query.QueryParameters.QUERY_MAX_CONCURRENT_TASKS;
import static datawave.microservice.query.QueryParameters.QUERY_MAX_RESULTS_OVERRIDE;
import static datawave.microservice.query.QueryParameters.QUERY_NAME;
import static datawave.microservice.query.QueryParameters.QUERY_PAGESIZE;
import static datawave.microservice.query.QueryParameters.QUERY_PAGETIMEOUT;
import static datawave.microservice.query.QueryParameters.QUERY_PARAMS;
import static datawave.microservice.query.QueryParameters.QUERY_PLAN_EXPAND_FIELDS;
import static datawave.microservice.query.QueryParameters.QUERY_PLAN_EXPAND_VALUES;
import static datawave.microservice.query.QueryParameters.QUERY_POOL;
import static datawave.microservice.query.QueryParameters.QUERY_STRING;
import static datawave.microservice.query.QueryParameters.QUERY_VISIBILITY;
import static datawave.microservice.query.lookup.LookupService.LOOKUP_CONTEXT;
import static datawave.microservice.query.lookup.LookupService.LOOKUP_STREAMING;
import static datawave.microservice.query.lookup.LookupService.LOOKUP_UUID_PAIRS;
import static datawave.microservice.query.translateid.TranslateIdService.TRANSLATE_ID;
import static datawave.query.QueryParameters.QUERY_SYNTAX;

import java.util.List;
import java.util.function.Supplier;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.annotation.Secured;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyEmitter;

import com.codahale.metrics.annotation.Timed;

import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.query.config.QueryProperties;
import datawave.microservice.query.lookup.LookupService;
import datawave.microservice.query.stream.StreamingProperties;
import datawave.microservice.query.stream.StreamingService;
import datawave.microservice.query.stream.listener.CountingResponseBodyEmitterListener;
import datawave.microservice.query.stream.listener.StreamingResponseListener;
import datawave.microservice.query.translateid.TranslateIdService;
import datawave.microservice.query.web.QuerySessionIdAdvice;
import datawave.microservice.query.web.annotation.ClearQuerySessionId;
import datawave.microservice.query.web.annotation.EnrichQueryMetrics;
import datawave.microservice.query.web.annotation.GenerateQuerySessionId;
import datawave.microservice.query.web.filter.BaseMethodStatsFilter;
import datawave.microservice.query.web.filter.CountingResponseBodyEmitter;
import datawave.microservice.query.web.filter.QueryMetricsEnrichmentFilterAdvice;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.GenericResponse;
import datawave.webservice.result.QueryImplListResponse;
import datawave.webservice.result.QueryLogicResponse;
import datawave.webservice.result.VoidResponse;
import io.swagger.v3.oas.annotations.ExternalDocumentation;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.headers.Header;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Query Controller /v1", description = "DataWave Query Management",
                externalDocs = @ExternalDocumentation(description = "Query Service Documentation",
                                url = "https://github.com/NationalSecurityAgency/datawave-query-service"))
@RestController
@RequestMapping(path = "/v1/query", produces = MediaType.APPLICATION_JSON_VALUE)
public class QueryController {
    private final QueryProperties queryProperties;
    private final QueryManagementService queryManagementService;
    private final LookupService lookupService;
    private final StreamingService streamingService;
    private final TranslateIdService translateIdService;
    
    private final StreamingProperties streamingProperties;
    
    private final Supplier<DatawaveUserDetails> serverUserDetailsSupplier;
    
    // Note: baseMethodStatsContext needs to be request scoped
    private final BaseMethodStatsFilter.BaseMethodStatsContext baseMethodStatsContext;
    // Note: queryMetricsEnrichmentContest needs to be request scoped
    private final QueryMetricsEnrichmentFilterAdvice.QueryMetricsEnrichmentContext queryMetricsEnrichmentContext;
    // Note: querySessionIdContext needs to be request scoped
    private final QuerySessionIdAdvice.QuerySessionIdContext querySessionIdContext;
    
    public QueryController(QueryProperties queryProperties, QueryManagementService queryManagementService, LookupService lookupService,
                    StreamingService streamingService, TranslateIdService translateIdService, StreamingProperties streamingProperties,
                    @Qualifier("serverUserDetailsSupplier") Supplier<DatawaveUserDetails> serverUserDetailsSupplier,
                    BaseMethodStatsFilter.BaseMethodStatsContext baseMethodStatsContext,
                    QueryMetricsEnrichmentFilterAdvice.QueryMetricsEnrichmentContext queryMetricsEnrichmentContext,
                    QuerySessionIdAdvice.QuerySessionIdContext querySessionIdContext) {
        this.queryProperties = queryProperties;
        this.queryManagementService = queryManagementService;
        this.lookupService = lookupService;
        this.streamingService = streamingService;
        this.translateIdService = translateIdService;
        this.streamingProperties = streamingProperties;
        this.serverUserDetailsSupplier = serverUserDetailsSupplier;
        this.baseMethodStatsContext = baseMethodStatsContext;
        this.queryMetricsEnrichmentContext = queryMetricsEnrichmentContext;
        this.querySessionIdContext = querySessionIdContext;
    }
    
    // @see QueryManagementService#define(String, MultiValueMap, String, DatawaveUserDetails)
    // @formatter:off
    @Operation(
            summary = "Defines a query using the given query logic and parameters.",
            description = "Defined queries cannot be started and run.<br>" +
                    "Auditing is not performed when defining a query.<br>" +
                    "Updates can be made to any parameter using <strong>update</strong>.<br>" +
                    "Create a runnable query from a defined query using <strong>duplicate</strong>, <strong>reset</strong>, or <strong>mapreduce/submit</strong>.<br>" +
                    "Delete a defined query using <strong>remove</strong>.<br>" +
                    "Aside from a limited set of admin actions, only the query owner can act on a defined query.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a generic response containing the query id",
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = GenericResponse.class)),
                    headers = {
                            @Header(
                                    name = "Pool",
                                    description = "the executor pool to target",
                                    schema = @Schema(defaultValue = "default"))}),
            @ApiResponse(
                    description = "if parameter validation fails<br>" +
                            "if query logic parameter validation fails<br>" +
                            "if security marking validation fails",
                    responseCode = "400",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the user doesn't have access to the requested query logic",
                    responseCode = "401",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if query storage fails<br>" +
                            "if there is an unknown error",
                    responseCode = "500",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class)))})
    @Parameters({
            @Parameter(
                    name = QUERY_BEGIN,
                    in = ParameterIn.QUERY,
                    description = "The query begin date",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "\"19660908 000000.000\""),
            @Parameter(
                    name = QUERY_END,
                    in = ParameterIn.QUERY,
                    description = "The query end date",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "\"20161002 235959.999\""),
            @Parameter(
                    name = QUERY_NAME,
                    in = ParameterIn.QUERY,
                    description = "The query name",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "Developer Test Query"),
            @Parameter(
                    name = QUERY_STRING,
                    in = ParameterIn.QUERY,
                    description = "The query string",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "GENRES:[Action to Western]"),
            @Parameter(
                    name = QUERY_AUTHORIZATIONS,
                    in = ParameterIn.QUERY,
                    description = "The query auths",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "PUBLIC,PRIVATE,BAR,FOO"),
            @Parameter(
                    name = QUERY_VISIBILITY,
                    in = ParameterIn.QUERY,
                    description = "The visibility to use when storing metrics for this query",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "PUBLIC"),
            @Parameter(
                    name = QUERY_SYNTAX,
                    in = ParameterIn.QUERY,
                    description = "The syntax used in the query",
                    schema = @Schema(implementation = String.class),
                    example = "LUCENE"),
            @Parameter(
                    name = QUERY_MAX_CONCURRENT_TASKS,
                    in = ParameterIn.QUERY,
                    description = "The max number of concurrent tasks to run for this query",
                    schema = @Schema(implementation = Integer.class),
                    example = "10"),
            @Parameter(
                    name = QUERY_POOL,
                    in = ParameterIn.QUERY,
                    description = "The executor pool to run against",
                    schema = @Schema(implementation = String.class),
                    example = "pool1"),
            @Parameter(
                    name = QUERY_PAGESIZE,
                    in = ParameterIn.QUERY,
                    description = "The requested page size",
                    schema = @Schema(implementation = Integer.class),
                    example = "10"),
            @Parameter(
                    name = QUERY_PAGETIMEOUT,
                    in = ParameterIn.QUERY,
                    description = "The call timeout when requesting a page, in minutes",
                    schema = @Schema(implementation = Integer.class),
                    example = "60"),
            @Parameter(
                    name = QUERY_MAX_RESULTS_OVERRIDE,
                    in = ParameterIn.QUERY,
                    description = "The max results override value",
                    schema = @Schema(implementation = Integer.class),
                    example = "5000"),
            @Parameter(
                    name = QUERY_PARAMS,
                    in = ParameterIn.QUERY,
                    description = "Additional query parameters",
                    schema = @Schema(implementation = String.class),
                    example = "KEY_1:VALUE_1;KEY_2:VALUE_2")
    })
    // @formatter:on
    @Timed(name = "dw.query.defineQuery", absolute = true)
    @GenerateQuerySessionId(cookieBasePath = "/query/v1/query/")
    @EnrichQueryMetrics(methodType = EnrichQueryMetrics.MethodType.CREATE)
    @RequestMapping(path = "{queryLogicName}/define", method = {RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public GenericResponse<String> define(@Parameter(description = "The query logic", example = "EventQuery") @PathVariable String queryLogicName,
                    @Parameter(hidden = true) @RequestParam MultiValueMap<String,String> parameters, @RequestHeader HttpHeaders headers,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) throws QueryException {
        GenericResponse<String> response = queryManagementService.define(queryLogicName, parameters, getPool(headers), currentUser);
        querySessionIdContext.setQueryId(response.getResult());
        return response;
    }
    
    // @see QueryManagementService#listQueryLogic(DatawaveUserDetails)
    @Operation(summary = "Gets a list of descriptions for the configured query logics, sorted by query logic name.",
                    description = "The descriptions include things like the audit type, optional and required parameters, required roles, and response class.")
    @Timed(name = "dw.query.listQueryLogic", absolute = true)
    @RequestMapping(path = "listQueryLogic", method = {RequestMethod.GET},
                    produces = {"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "text/html"})
    public QueryLogicResponse listQueryLogic(@AuthenticationPrincipal DatawaveUserDetails currentUser) {
        return queryManagementService.listQueryLogic(currentUser);
    }
    
    // @see QueryManagementService#create(String, MultiValueMap, String, DatawaveUserDetails)
    // @formatter:off
    @Operation(
            summary = "Creates a query using the given query logic and parameters.",
            description = "Created queries will start running immediately.<br>" +
                    "Auditing is performed before the query is started.<br>" +
                    "Query results can be retrieved using <strong>next</strong>.<br>" +
                    "Updates can be made to any parameter which doesn't affect the scope of the query using <strong>update</strong>.<br>" +
                    "Stop a running query gracefully using <strong>close</strong> or forcefully using <strong>cancel</strong>.<br>" +
                    "Stop, and restart a running query using <strong>reset</strong>.<br>" +
                    "Create a copy of a running query using <strong>duplicate</strong>.<br>" +
                    "Aside from a limited set of admin actions, only the query owner can act on a running query.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a generic response containing the query id",
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = GenericResponse.class)),
                    headers = {
                            @Header(
                                    name = "Pool",
                                    description = "the executor pool to target",
                                    schema = @Schema(defaultValue = "default"))}),
            @ApiResponse(
                    description = "if parameter validation fails<br>" +
                            "if query logic parameter validation fails<br>" +
                            "if security marking validation fails<br>" +
                            "if auditing fails",
                    responseCode = "400",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the user doesn't have access to the requested query logic",
                    responseCode = "401",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if query storage fails<br>" +
                            "if there is an unknown error",
                    responseCode = "500",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class)))})
    @Parameters({
            @Parameter(
                    name = QUERY_BEGIN,
                    in = ParameterIn.QUERY,
                    description = "The query begin date",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "\"19660908 000000.000\""),
            @Parameter(
                    name = QUERY_END,
                    in = ParameterIn.QUERY,
                    description = "The query end date",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "\"20161002 235959.999\""),
            @Parameter(
                    name = QUERY_NAME,
                    in = ParameterIn.QUERY,
                    description = "The query name",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "Developer Test Query"),
            @Parameter(
                    name = QUERY_STRING,
                    in = ParameterIn.QUERY,
                    description = "The query string",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "GENRES:[Action to Western]"),
            @Parameter(
                    name = QUERY_AUTHORIZATIONS,
                    in = ParameterIn.QUERY,
                    description = "The query auths",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "PUBLIC,PRIVATE,BAR,FOO"),
            @Parameter(
                    name = QUERY_VISIBILITY,
                    in = ParameterIn.QUERY,
                    description = "The visibility to use when storing metrics for this query",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "PUBLIC"),
            @Parameter(
                    name = QUERY_SYNTAX,
                    in = ParameterIn.QUERY,
                    description = "The syntax used in the query",
                    schema = @Schema(implementation = String.class),
                    example = "LUCENE"),
            @Parameter(
                    name = QUERY_MAX_CONCURRENT_TASKS,
                    in = ParameterIn.QUERY,
                    description = "The max number of concurrent tasks to run for this query",
                    schema = @Schema(implementation = Integer.class),
                    example = "10"),
            @Parameter(
                    name = QUERY_POOL,
                    in = ParameterIn.QUERY,
                    description = "The executor pool to run against",
                    schema = @Schema(implementation = String.class),
                    example = "pool1"),
            @Parameter(
                    name = QUERY_PAGESIZE,
                    in = ParameterIn.QUERY,
                    description = "The requested page size",
                    schema = @Schema(implementation = Integer.class),
                    example = "10"),
            @Parameter(
                    name = QUERY_PAGETIMEOUT,
                    in = ParameterIn.QUERY,
                    description = "The call timeout when requesting a page, in minutes",
                    schema = @Schema(implementation = Integer.class),
                    example = "60"),
            @Parameter(
                    name = QUERY_MAX_RESULTS_OVERRIDE,
                    in = ParameterIn.QUERY,
                    description = "The max results override value",
                    schema = @Schema(implementation = Integer.class),
                    example = "5000"),
            @Parameter(
                    name = QUERY_PARAMS,
                    in = ParameterIn.QUERY,
                    description = "Additional query parameters",
                    schema = @Schema(implementation = String.class),
                    example = "KEY_1:VALUE_1;KEY_2:VALUE_2")
    })
    // @formatter:on
    @Timed(name = "dw.query.createQuery", absolute = true)
    @GenerateQuerySessionId(cookieBasePath = "/query/v1/query/")
    @EnrichQueryMetrics(methodType = EnrichQueryMetrics.MethodType.CREATE)
    @RequestMapping(path = "{queryLogic}/create", method = {RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public GenericResponse<String> create(@Parameter(description = "The query logic", example = "EventQuery") @PathVariable String queryLogic,
                    @Parameter(hidden = true) @RequestParam MultiValueMap<String,String> parameters, @RequestHeader HttpHeaders headers,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) throws QueryException {
        GenericResponse<String> response = queryManagementService.create(queryLogic, parameters, getPool(headers), currentUser);
        querySessionIdContext.setQueryId(response.getResult());
        return response;
    }
    
    // @see QueryManagementService#plan(String, MultiValueMap, String, DatawaveUserDetails)
    // @formatter:off
    @Operation(
            summary = "Generates a query plan using the given query logic and parameters.",
            description = "Created queries will begin planning immediately.<br>" +
                    "Auditing is performed if we are expanding indices.<br>" +
                    "Query plan will be returned in the response.<br>" +
                    "Updates can be made to any parameter which doesn't affect the scope of the query using <strong>update</strong>.<br>" +
                    "Stop a running query gracefully using <strong>close</strong> or forcefully using <strong>cancel</strong>.<br>" +
                    "Stop, and restart a running query using <strong>reset</strong>.<br>" +
                    "Create a copy of a running query using <strong>duplicate</strong>.<br>" +
                    "Aside from a limited set of admin actions, only the query owner can act on a running query.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a generic response containing the query plan",
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = GenericResponse.class)),
                    headers = {
                            @Header(
                                    name = "Pool",
                                    description = "the executor pool to target",
                                    schema = @Schema(defaultValue = "default"))}),
            @ApiResponse(
                    description = "if parameter validation fails<br>" +
                            "if query logic parameter validation fails<br>" +
                            "if security marking validation fails<br>" +
                            "if auditing fails",
                    responseCode = "400",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the user doesn't have access to the requested query logic",
                    responseCode = "401",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if query storage fails<br>" +
                            "if there is an unknown error",
                    responseCode = "500",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class)))})
    @Parameters({
            @Parameter(
                    name = QUERY_BEGIN,
                    in = ParameterIn.QUERY,
                    description = "The query begin date",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "\"19660908 000000.000\""),
            @Parameter(
                    name = QUERY_END,
                    in = ParameterIn.QUERY,
                    description = "The query end date",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "\"20161002 235959.999\""),
            @Parameter(
                    name = QUERY_NAME,
                    in = ParameterIn.QUERY,
                    description = "The query name",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "Developer Test Query"),
            @Parameter(
                    name = QUERY_STRING,
                    in = ParameterIn.QUERY,
                    description = "The query string",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "GENRES:[Action to Western]"),
            @Parameter(
                    name = QUERY_AUTHORIZATIONS,
                    in = ParameterIn.QUERY,
                    description = "The query auths",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "PUBLIC,PRIVATE,BAR,FOO"),
            @Parameter(
                    name = QUERY_VISIBILITY,
                    in = ParameterIn.QUERY,
                    description = "The visibility to use when storing metrics for this query",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "PUBLIC"),
            @Parameter(
                    name = QUERY_PLAN_EXPAND_FIELDS,
                    in = ParameterIn.QUERY,
                    description = "Whether to expand unfielded terms",
                    schema = @Schema(implementation = Boolean.class),
                    example = "true"
            ),
            @Parameter(
                    name = QUERY_PLAN_EXPAND_VALUES,
                    in = ParameterIn.QUERY,
                    description = "Whether to expand regex and/or ranges into discrete values<br>" +
                            "If 'true', auditing will be performed",
                    schema = @Schema(implementation = Boolean.class),
                    example = "true"
            ),
            @Parameter(
                    name = QUERY_SYNTAX,
                    in = ParameterIn.QUERY,
                    description = "The syntax used in the query",
                    schema = @Schema(implementation = String.class),
                    example = "LUCENE"),
            @Parameter(
                    name = QUERY_MAX_CONCURRENT_TASKS,
                    in = ParameterIn.QUERY,
                    description = "The max number of concurrent tasks to run for this query",
                    schema = @Schema(implementation = Integer.class),
                    example = "10"),
            @Parameter(
                    name = QUERY_POOL,
                    in = ParameterIn.QUERY,
                    description = "The executor pool to run against",
                    schema = @Schema(implementation = String.class),
                    example = "pool1"),
            @Parameter(
                    name = QUERY_PAGESIZE,
                    in = ParameterIn.QUERY,
                    description = "The requested page size",
                    schema = @Schema(implementation = Integer.class),
                    example = "10"),
            @Parameter(
                    name = QUERY_PAGETIMEOUT,
                    in = ParameterIn.QUERY,
                    description = "The call timeout when requesting a page, in minutes",
                    schema = @Schema(implementation = Integer.class),
                    example = "60"),
            @Parameter(
                    name = QUERY_MAX_RESULTS_OVERRIDE,
                    in = ParameterIn.QUERY,
                    description = "The max results override value",
                    schema = @Schema(implementation = Integer.class),
                    example = "5000"),
            @Parameter(
                    name = QUERY_PARAMS,
                    in = ParameterIn.QUERY,
                    description = "Additional query parameters",
                    schema = @Schema(implementation = String.class),
                    example = "KEY_1:VALUE_1;KEY_2:VALUE_2")
    })
    // @formatter:on
    @Timed(name = "dw.query.planQuery", absolute = true)
    @RequestMapping(path = "{queryLogic}/plan", method = {RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public GenericResponse<String> plan(@Parameter(description = "The query logic", example = "EventQuery") @PathVariable String queryLogic,
                    @Parameter(hidden = true) @RequestParam MultiValueMap<String,String> parameters, @RequestHeader HttpHeaders headers,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) throws QueryException {
        return queryManagementService.plan(queryLogic, parameters, getPool(headers), currentUser);
    }
    
    // @see QueryManagementService#predict(String, MultiValueMap, String, DatawaveUserDetails)
    // @formatter:off
    @Operation(
            summary = "Generates a query prediction using the given query logic and parameters.",
            description = "Created queries will begin predicting immediately.<br>" +
                    "Auditing is not performed.<br>" +
                    "Query prediction will be returned in the response.<br>" +
                    "Updates can be made to any parameter which doesn't affect the scope of the query using <strong>update</strong>.<br>")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a generic response containing the query plan",
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = GenericResponse.class)),
                    headers = {
                            @Header(
                                    name = "Pool",
                                    description = "the executor pool to target",
                                    schema = @Schema(defaultValue = "default"))}),
            @ApiResponse(
                    description = "if parameter validation fails<br>" +
                            "if query logic parameter validation fails<br>" +
                            "if security marking validation fails<br>" +
                            "if auditing fails",
                    responseCode = "400",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the user doesn't have access to the requested query logic",
                    responseCode = "401",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if query storage fails<br>" +
                            "if there is an unknown error",
                    responseCode = "500",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class)))})
    @Parameters({
            @Parameter(
                    name = QUERY_BEGIN,
                    in = ParameterIn.QUERY,
                    description = "The query begin date",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "\"19660908 000000.000\""),
            @Parameter(
                    name = QUERY_END,
                    in = ParameterIn.QUERY,
                    description = "The query end date",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "\"20161002 235959.999\""),
            @Parameter(
                    name = QUERY_NAME,
                    in = ParameterIn.QUERY,
                    description = "The query name",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "Developer Test Query"),
            @Parameter(
                    name = QUERY_STRING,
                    in = ParameterIn.QUERY,
                    description = "The query string",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "GENRES:[Action to Western]"),
            @Parameter(
                    name = QUERY_AUTHORIZATIONS,
                    in = ParameterIn.QUERY,
                    description = "The query auths",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "PUBLIC,PRIVATE,BAR,FOO"),
            @Parameter(
                    name = QUERY_VISIBILITY,
                    in = ParameterIn.QUERY,
                    description = "The visibility to use when storing metrics for this query",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "PUBLIC"),
            @Parameter(
                    name = QUERY_SYNTAX,
                    in = ParameterIn.QUERY,
                    description = "The syntax used in the query",
                    schema = @Schema(implementation = String.class),
                    example = "LUCENE"),
            @Parameter(
                    name = QUERY_MAX_CONCURRENT_TASKS,
                    in = ParameterIn.QUERY,
                    description = "The max number of concurrent tasks to run for this query",
                    schema = @Schema(implementation = Integer.class),
                    example = "10"),
            @Parameter(
                    name = QUERY_POOL,
                    in = ParameterIn.QUERY,
                    description = "The executor pool to run against",
                    schema = @Schema(implementation = String.class),
                    example = "pool1"),
            @Parameter(
                    name = QUERY_PAGESIZE,
                    in = ParameterIn.QUERY,
                    description = "The requested page size",
                    schema = @Schema(implementation = Integer.class),
                    example = "10"),
            @Parameter(
                    name = QUERY_PAGETIMEOUT,
                    in = ParameterIn.QUERY,
                    description = "The call timeout when requesting a page, in minutes",
                    schema = @Schema(implementation = Integer.class),
                    example = "60"),
            @Parameter(
                    name = QUERY_MAX_RESULTS_OVERRIDE,
                    in = ParameterIn.QUERY,
                    description = "The max results override value",
                    schema = @Schema(implementation = Integer.class),
                    example = "5000"),
            @Parameter(
                    name = QUERY_PARAMS,
                    in = ParameterIn.QUERY,
                    description = "Additional query parameters",
                    schema = @Schema(implementation = String.class),
                    example = "KEY_1:VALUE_1;KEY_2:VALUE_2")
    })
    // @formatter:on
    @Timed(name = "dw.query.predictQuery", absolute = true)
    @RequestMapping(path = "{queryLogic}/predict", method = {RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public GenericResponse<String> predict(@Parameter(description = "The query logic", example = "EventQuery") @PathVariable String queryLogic,
                    @Parameter(hidden = true) @RequestParam MultiValueMap<String,String> parameters, @RequestHeader HttpHeaders headers,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) throws QueryException {
        return queryManagementService.predict(queryLogic, parameters, getPool(headers), currentUser);
    }
    
    // @see LookupService#lookupUUID(MultiValueMap, String, DatawaveUserDetails)
    // @see LookupService#lookupUUID(MultiValueMap, String, DatawaveUserDetails, StreamingResponseListener)
    // @formatter:off
    @Operation(
            summary = "Creates an event lookup query using the query logic associated with the given uuid type(s) and parameters, and returns the first page of results.",
            description = "Lookup queries will start running immediately.<br>" +
                    "Auditing is performed before the query is started.<br>" +
                    "Each of the uuid pairs must map to the same query logic.<br>" +
                    "After the first page is returned, the query will be closed.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a base query response containing the first page of results",
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = BaseQueryResponse.class)),
                    headers = {
                            @Header(
                                    name = "Pool",
                                    description = "the executor pool to target",
                                    schema = @Schema(defaultValue = "default"))}),
            @ApiResponse(
                    description = "if no query results are found",
                    responseCode = "204",
                    content = @Content(schema = @Schema(hidden = true))),
            @ApiResponse(
                    description = "if parameter validation fails<br>" +
                            "if query logic parameter validation fails<br>" +
                            "if security marking validation fails<br>" +
                            "if auditing fails",
                    responseCode = "400",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the user doesn't have access to the requested query logic",
                    responseCode = "401",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if query storage fails<br>" +
                            "if the next call times out<br>" +
                            "if the next task is rejected by the executor<br>" +
                            "if there is an unknown error",
                    responseCode = "500",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class)))})
    @Parameters({
            @Parameter(
                    name = LOOKUP_STREAMING,
                    in = ParameterIn.QUERY,
                    description = "if true, streams all results back",
                    schema = @Schema(implementation = Boolean.class),
                    example = "true"),
            @Parameter(
                    name = LOOKUP_CONTEXT,
                    in = ParameterIn.QUERY,
                    description = "The lookup UUID type context",
                    example = "default",
                    array = @ArraySchema(schema = @Schema(implementation = String.class))),
            @Parameter(
                    name = QUERY_BEGIN,
                    in = ParameterIn.QUERY,
                    description = "The query begin date",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "\"19660908 000000.000\""),
            @Parameter(
                    name = QUERY_END,
                    in = ParameterIn.QUERY,
                    description = "The query end date",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "\"20161002 235959.999\""),
            @Parameter(
                    name = QUERY_NAME,
                    in = ParameterIn.QUERY,
                    description = "The query name",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "Developer Test Query"),
            @Parameter(
                    name = QUERY_AUTHORIZATIONS,
                    in = ParameterIn.QUERY,
                    description = "The query auths",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "PUBLIC,PRIVATE,BAR,FOO"),
            @Parameter(
                    name = QUERY_VISIBILITY,
                    in = ParameterIn.QUERY,
                    description = "The visibility to use when storing metrics for this query",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "PUBLIC"),
            @Parameter(
                    name = QUERY_MAX_CONCURRENT_TASKS,
                    in = ParameterIn.QUERY,
                    description = "The max number of concurrent tasks to run for this query",
                    schema = @Schema(implementation = Integer.class),
                    example = "10"),
            @Parameter(
                    name = QUERY_POOL,
                    in = ParameterIn.QUERY,
                    description = "The executor pool to run against",
                    schema = @Schema(implementation = String.class),
                    example = "pool1"),
            @Parameter(
                    name = QUERY_PAGESIZE,
                    in = ParameterIn.QUERY,
                    description = "The requested page size",
                    schema = @Schema(implementation = Integer.class),
                    example = "10"),
            @Parameter(
                    name = QUERY_PAGETIMEOUT,
                    in = ParameterIn.QUERY,
                    description = "The call timeout when requesting a page, in minutes",
                    schema = @Schema(implementation = Integer.class),
                    example = "60"),
            @Parameter(
                    name = QUERY_MAX_RESULTS_OVERRIDE,
                    in = ParameterIn.QUERY,
                    description = "The max results override value",
                    schema = @Schema(implementation = Integer.class),
                    example = "5000"),
            @Parameter(
                    name = QUERY_PARAMS,
                    in = ParameterIn.QUERY,
                    description = "Additional query parameters",
                    schema = @Schema(implementation = String.class),
                    example = "KEY_1:VALUE_1;KEY_2:VALUE_2")
    })
    // @formatter:on
    @Timed(name = "dw.query.lookupUUID", absolute = true)
    @RequestMapping(path = "lookupUUID/{uuidType}/{uuid}", method = {RequestMethod.GET}, produces = {"application/xml", "text/xml", "application/json",
            "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public Object lookupUUID(@Parameter(description = "The UUID type", example = "PAGE_TITLE") @PathVariable(required = false) String uuidType,
                    @Parameter(description = "The UUID", example = "anarchism") @PathVariable(required = false) String uuid,
                    @Parameter(hidden = true) @RequestParam MultiValueMap<String,String> parameters, @RequestHeader HttpHeaders headers,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) throws QueryException {
        parameters.add(LOOKUP_UUID_PAIRS, String.join(LOOKUP_KEY_VALUE_DELIMITER, uuidType, uuid));
        
        if (Boolean.parseBoolean(parameters.getFirst(LOOKUP_STREAMING))) {
            MediaType contentType = determineContentType(headers.getAccept(), MediaType.parseMediaType(streamingProperties.getDefaultContentType()));
            CountingResponseBodyEmitter emitter = baseMethodStatsContext.createCountingResponseBodyEmitter(streamingProperties.getCallTimeoutMillis());
            lookupService.lookupUUID(parameters, getPool(headers), currentUser, new CountingResponseBodyEmitterListener(emitter, contentType));
            return emitter;
        } else {
            return lookupService.lookupUUID(parameters, getPool(headers), currentUser);
        }
    }
    
    // @see LookupService#lookupUUID(MultiValueMap, String, DatawaveUserDetails)
    // @see LookupService#lookupUUID(MultiValueMap, String, DatawaveUserDetails, StreamingResponseListener)
    // @formatter:off
    @Operation(
            summary = "Creates an event lookup query using the query logic associated with the given uuid type(s) and parameters, and returns the first page of results.",
            description = "Lookup queries will start running immediately.<br>" +
                    "Auditing is performed before the query is started.<br>" +
                    "Each of the uuid pairs must map to the same query logic.<br>" +
                    "After the first page is returned, the query will be closed.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a base query response containing the first page of results",
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = BaseQueryResponse.class)),
                    headers = {
                            @Header(
                                    name = "Pool",
                                    description = "the executor pool to target",
                                    schema = @Schema(defaultValue = "default"))}),
            @ApiResponse(
                    description = "if no query results are found",
                    responseCode = "204",
                    content = @Content(schema = @Schema(hidden = true))),
            @ApiResponse(
                    description = "if parameter validation fails<br>" +
                            "if query logic parameter validation fails<br>" +
                            "if security marking validation fails<br>" +
                            "if auditing fails",
                    responseCode = "400",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the user doesn't have access to the requested query logic",
                    responseCode = "401",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if query storage fails<br>" +
                            "if the next call times out<br>" +
                            "if the next task is rejected by the executor<br>" +
                            "if there is an unknown error",
                    responseCode = "500",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class)))})
    @Parameters({
            @Parameter(
                    name = LOOKUP_STREAMING,
                    in = ParameterIn.QUERY,
                    description = "if true, streams all results back",
                    schema = @Schema(implementation = Boolean.class),
                    example = "true"),
            @Parameter(
                    name = LOOKUP_CONTEXT,
                    in = ParameterIn.QUERY,
                    description = "The lookup UUID type context",
                    example = "default",
                    array = @ArraySchema(schema = @Schema(implementation = String.class))),
            @Parameter(
                    name = LOOKUP_UUID_PAIRS,
                    in = ParameterIn.QUERY,
                    description = "The lookup UUID pairs<br>" +
                            "To lookup multiple UUID pairs, submit multiples of this parameter",
                    required = true,
                    example = "PAGE_TITLE:anarchism",
                    array = @ArraySchema(schema = @Schema(implementation = String.class))),
            @Parameter(
                    name = QUERY_BEGIN,
                    in = ParameterIn.QUERY,
                    description = "The query begin date",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "\"19660908 000000.000\""),
            @Parameter(
                    name = QUERY_END,
                    in = ParameterIn.QUERY,
                    description = "The query end date",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "\"20161002 235959.999\""),
            @Parameter(
                    name = QUERY_NAME,
                    in = ParameterIn.QUERY,
                    description = "The query name",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "Developer Test Query"),
            @Parameter(
                    name = QUERY_AUTHORIZATIONS,
                    in = ParameterIn.QUERY,
                    description = "The query auths",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "PUBLIC,PRIVATE,BAR,FOO"),
            @Parameter(
                    name = QUERY_VISIBILITY,
                    in = ParameterIn.QUERY,
                    description = "The visibility to use when storing metrics for this query",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "PUBLIC"),
            @Parameter(
                    name = QUERY_MAX_CONCURRENT_TASKS,
                    in = ParameterIn.QUERY,
                    description = "The max number of concurrent tasks to run for this query",
                    schema = @Schema(implementation = Integer.class),
                    example = "10"),
            @Parameter(
                    name = QUERY_POOL,
                    in = ParameterIn.QUERY,
                    description = "The executor pool to run against",
                    schema = @Schema(implementation = String.class),
                    example = "pool1"),
            @Parameter(
                    name = QUERY_PAGESIZE,
                    in = ParameterIn.QUERY,
                    description = "The requested page size",
                    schema = @Schema(implementation = Integer.class),
                    example = "10"),
            @Parameter(
                    name = QUERY_PAGETIMEOUT,
                    in = ParameterIn.QUERY,
                    description = "The call timeout when requesting a page, in minutes",
                    schema = @Schema(implementation = Integer.class),
                    example = "60"),
            @Parameter(
                    name = QUERY_MAX_RESULTS_OVERRIDE,
                    in = ParameterIn.QUERY,
                    description = "The max results override value",
                    schema = @Schema(implementation = Integer.class),
                    example = "5000"),
            @Parameter(
                    name = QUERY_PARAMS,
                    in = ParameterIn.QUERY,
                    description = "Additional query parameters",
                    schema = @Schema(implementation = String.class),
                    example = "KEY_1:VALUE_1;KEY_2:VALUE_2")
    })
    // @formatter:on
    @Timed(name = "dw.query.lookupUUIDBatch", absolute = true)
    @RequestMapping(path = "lookupUUID", method = {RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public Object lookupUUIDBatch(@Parameter(hidden = true) @RequestParam MultiValueMap<String,String> parameters, @RequestHeader HttpHeaders headers,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) throws QueryException {
        if (Boolean.parseBoolean(parameters.getFirst(LOOKUP_STREAMING))) {
            MediaType contentType = determineContentType(headers.getAccept(), MediaType.parseMediaType(streamingProperties.getDefaultContentType()));
            CountingResponseBodyEmitter emitter = baseMethodStatsContext.createCountingResponseBodyEmitter(streamingProperties.getCallTimeoutMillis());
            lookupService.lookupUUID(parameters, getPool(headers), currentUser, new CountingResponseBodyEmitterListener(emitter, contentType));
            return emitter;
        } else {
            return lookupService.lookupUUID(parameters, getPool(headers), currentUser);
        }
    }
    
    // @see LookupService#lookupContentUUID(MultiValueMap, String, DatawaveUserDetails)
    // @see LookupService#lookupContentUUID(MultiValueMap, String, DatawaveUserDetails, StreamingResponseListener)
    // @formatter:off
    @Operation(
            summary = "Creates a content lookup query using the query logic associated with the given uuid type(s) and parameters, and returns the first page of results.",
            description = "Lookup queries will start running immediately.<br>" +
                    "Auditing is performed before the query is started.<br>" +
                    "Each of the uuid pairs must map to the same query logic.<br>" +
                    "After the first page is returned, the query will be closed.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a base query response containing the first page of results",
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = BaseQueryResponse.class)),
                    headers = {
                            @Header(
                                    name = "Pool",
                                    description = "the executor pool to target",
                                    schema = @Schema(defaultValue = "default"))}),
            @ApiResponse(
                    description = "if no query results are found",
                    responseCode = "204",
                    content = @Content(schema = @Schema(hidden = true))),
            @ApiResponse(
                    description = "if parameter validation fails<br>" +
                            "if query logic parameter validation fails<br>" +
                            "if security marking validation fails<br>" +
                            "if auditing fails",
                    responseCode = "400",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the user doesn't have access to the requested query logic",
                    responseCode = "401",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if query storage fails<br>" +
                            "if the next call times out<br>" +
                            "if the next task is rejected by the executor<br>" +
                            "if there is an unknown error",
                    responseCode = "500",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class)))})
    @Parameters({
            @Parameter(
                    name = LOOKUP_STREAMING,
                    in = ParameterIn.QUERY,
                    description = "if true, streams all results back",
                    schema = @Schema(implementation = Boolean.class),
                    example = "true"),
            @Parameter(
                    name = LOOKUP_CONTEXT,
                    in = ParameterIn.QUERY,
                    description = "The lookup UUID type context",
                    example = "default",
                    array = @ArraySchema(schema = @Schema(implementation = String.class))),
            @Parameter(
                    name = QUERY_BEGIN,
                    in = ParameterIn.QUERY,
                    description = "The query begin date",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "\"19660908 000000.000\""),
            @Parameter(
                    name = QUERY_END,
                    in = ParameterIn.QUERY,
                    description = "The query end date",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "\"20161002 235959.999\""),
            @Parameter(
                    name = QUERY_NAME,
                    in = ParameterIn.QUERY,
                    description = "The query name",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "Developer Test Query"),
            @Parameter(
                    name = QUERY_AUTHORIZATIONS,
                    in = ParameterIn.QUERY,
                    description = "The query auths",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "PUBLIC,PRIVATE,BAR,FOO"),
            @Parameter(
                    name = QUERY_VISIBILITY,
                    in = ParameterIn.QUERY,
                    description = "The visibility to use when storing metrics for this query",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "PUBLIC"),
            @Parameter(
                    name = QUERY_MAX_CONCURRENT_TASKS,
                    in = ParameterIn.QUERY,
                    description = "The max number of concurrent tasks to run for this query",
                    schema = @Schema(implementation = Integer.class),
                    example = "10"),
            @Parameter(
                    name = QUERY_POOL,
                    in = ParameterIn.QUERY,
                    description = "The executor pool to run against",
                    schema = @Schema(implementation = String.class),
                    example = "pool1"),
            @Parameter(
                    name = QUERY_PAGESIZE,
                    in = ParameterIn.QUERY,
                    description = "The requested page size",
                    schema = @Schema(implementation = Integer.class),
                    example = "10"),
            @Parameter(
                    name = QUERY_PAGETIMEOUT,
                    in = ParameterIn.QUERY,
                    description = "The call timeout when requesting a page, in minutes",
                    schema = @Schema(implementation = Integer.class),
                    example = "60"),
            @Parameter(
                    name = QUERY_MAX_RESULTS_OVERRIDE,
                    in = ParameterIn.QUERY,
                    description = "The max results override value",
                    schema = @Schema(implementation = Integer.class),
                    example = "5000"),
            @Parameter(
                    name = QUERY_PARAMS,
                    in = ParameterIn.QUERY,
                    description = "Additional query parameters",
                    schema = @Schema(implementation = String.class),
                    example = "KEY_1:VALUE_1;KEY_2:VALUE_2")
    })
    // @formatter:on
    @Timed(name = "dw.query.lookupContentUUID", absolute = true)
    @RequestMapping(path = "lookupContentUUID/{uuidType}/{uuid}", method = {RequestMethod.GET}, produces = {"application/xml", "text/xml", "application/json",
            "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public Object lookupContentUUID(@Parameter(description = "The UUID type", example = "PAGE_TITLE") @PathVariable(required = false) String uuidType,
                    @Parameter(description = "The UUID", example = "anarchism") @PathVariable(required = false) String uuid,
                    @Parameter(hidden = true) @RequestParam MultiValueMap<String,String> parameters, @RequestHeader HttpHeaders headers,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) throws QueryException {
        parameters.add(LOOKUP_UUID_PAIRS, String.join(LOOKUP_KEY_VALUE_DELIMITER, uuidType, uuid));
        
        if (Boolean.parseBoolean(parameters.getFirst(LOOKUP_STREAMING))) {
            MediaType contentType = determineContentType(headers.getAccept(), MediaType.parseMediaType(streamingProperties.getDefaultContentType()));
            CountingResponseBodyEmitter emitter = baseMethodStatsContext.createCountingResponseBodyEmitter(streamingProperties.getCallTimeoutMillis());
            lookupService.lookupContentUUID(parameters, getPool(headers), currentUser, new CountingResponseBodyEmitterListener(emitter, contentType));
            return emitter;
        } else {
            return lookupService.lookupContentUUID(parameters, getPool(headers), currentUser);
        }
    }
    
    // @see LookupService#lookupContentUUID(MultiValueMap, String, DatawaveUserDetails)
    // @see LookupService#lookupContentUUID(MultiValueMap, String, DatawaveUserDetails)
    // @formatter:off
    @Operation(
            summary = "Creates a batch content lookup query using the query logic associated with the given uuid type(s) and parameters, and returns the first page of results.",
            description = "Lookup queries will start running immediately.<br>" +
                    "Auditing is performed before the query is started.<br>" +
                    "Each of the uuid pairs must map to the same query logic.<br>" +
                    "After the first page is returned, the query will be closed.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a base query response containing the first page of results",
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = BaseQueryResponse.class)),
                    headers = {
                            @Header(
                                    name = "Pool",
                                    description = "the executor pool to target",
                                    schema = @Schema(defaultValue = "default"))}),
            @ApiResponse(
                    description = "if no query results are found",
                    responseCode = "204",
                    content = @Content(schema = @Schema(hidden = true))),
            @ApiResponse(
                    description = "if parameter validation fails<br>" +
                            "if query logic parameter validation fails<br>" +
                            "if security marking validation fails<br>" +
                            "if auditing fails",
                    responseCode = "400",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the user doesn't have access to the requested query logic",
                    responseCode = "401",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if query storage fails<br>" +
                            "if the next call times out<br>" +
                            "if the next task is rejected by the executor<br>" +
                            "if there is an unknown error",
                    responseCode = "500",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class)))})
    @Parameters({
            @Parameter(
                    name = LOOKUP_STREAMING,
                    in = ParameterIn.QUERY,
                    description = "if true, streams all results back",
                    schema = @Schema(implementation = Boolean.class),
                    example = "true"),
            @Parameter(
                    name = LOOKUP_CONTEXT,
                    in = ParameterIn.QUERY,
                    description = "The lookup UUID type context",
                    example = "default",
                    array = @ArraySchema(schema = @Schema(implementation = String.class))),
            @Parameter(
                    name = LOOKUP_UUID_PAIRS,
                    in = ParameterIn.QUERY,
                    description = "The lookup UUID pairs<br>" +
                            "To lookup multiple UUID pairs, submit multiples of this parameter",
                    required = true,
                    example = "PAGE_TITLE:anarchism",
                    array = @ArraySchema(schema = @Schema(implementation = String.class))),
            @Parameter(
                    name = QUERY_BEGIN,
                    in = ParameterIn.QUERY,
                    description = "The query begin date",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "\"19660908 000000.000\""),
            @Parameter(
                    name = QUERY_END,
                    in = ParameterIn.QUERY,
                    description = "The query end date",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "\"20161002 235959.999\""),
            @Parameter(
                    name = QUERY_NAME,
                    in = ParameterIn.QUERY,
                    description = "The query name",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "Developer Test Query"),
            @Parameter(
                    name = QUERY_AUTHORIZATIONS,
                    in = ParameterIn.QUERY,
                    description = "The query auths",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "PUBLIC,PRIVATE,BAR,FOO"),
            @Parameter(
                    name = QUERY_VISIBILITY,
                    in = ParameterIn.QUERY,
                    description = "The visibility to use when storing metrics for this query",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "PUBLIC"),
            @Parameter(
                    name = QUERY_MAX_CONCURRENT_TASKS,
                    in = ParameterIn.QUERY,
                    description = "The max number of concurrent tasks to run for this query",
                    schema = @Schema(implementation = Integer.class),
                    example = "10"),
            @Parameter(
                    name = QUERY_POOL,
                    in = ParameterIn.QUERY,
                    description = "The executor pool to run against",
                    schema = @Schema(implementation = String.class),
                    example = "pool1"),
            @Parameter(
                    name = QUERY_PAGESIZE,
                    in = ParameterIn.QUERY,
                    description = "The requested page size",
                    schema = @Schema(implementation = Integer.class),
                    example = "10"),
            @Parameter(
                    name = QUERY_PAGETIMEOUT,
                    in = ParameterIn.QUERY,
                    description = "The call timeout when requesting a page, in minutes",
                    schema = @Schema(implementation = Integer.class),
                    example = "60"),
            @Parameter(
                    name = QUERY_MAX_RESULTS_OVERRIDE,
                    in = ParameterIn.QUERY,
                    description = "The max results override value",
                    schema = @Schema(implementation = Integer.class),
                    example = "5000"),
            @Parameter(
                    name = QUERY_PARAMS,
                    in = ParameterIn.QUERY,
                    description = "Additional query parameters",
                    schema = @Schema(implementation = String.class),
                    example = "KEY_1:VALUE_1;KEY_2:VALUE_2")
    })
    // @formatter:on
    @Timed(name = "dw.query.lookupContentUUIDBatch", absolute = true)
    @RequestMapping(path = "lookupContentUUID", method = {RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public Object lookupContentUUIDBatch(@Parameter(hidden = true) @RequestParam MultiValueMap<String,String> parameters, @RequestHeader HttpHeaders headers,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) throws QueryException {
        if (Boolean.parseBoolean(parameters.getFirst(LOOKUP_STREAMING))) {
            MediaType contentType = determineContentType(headers.getAccept(), MediaType.parseMediaType(streamingProperties.getDefaultContentType()));
            CountingResponseBodyEmitter emitter = baseMethodStatsContext.createCountingResponseBodyEmitter(streamingProperties.getCallTimeoutMillis());
            lookupService.lookupContentUUID(parameters, getPool(headers), currentUser, new CountingResponseBodyEmitterListener(emitter, contentType));
            return emitter;
        } else {
            return lookupService.lookupContentUUID(parameters, getPool(headers), currentUser);
        }
    }
    
    // @see TranslateIdService#translateId(String, MultiValueMap, String, DatawaveUserDetails)
    // @formatter:off
    @Operation(
            summary = "Get one or more ID(s), if any, that correspond to the given ID.",
            description = "This method only returns the first page, so set pagesize appropriately.<br>" +
                    "Since the underlying query is automatically closed, callers are NOT expected to request additional pages or close the query.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a base query response containing the first page of results",
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = BaseQueryResponse.class)),
                    headers = {
                            @Header(
                                    name = "Pool",
                                    description = "the executor pool to target",
                                    schema = @Schema(defaultValue = "default"))}),
            @ApiResponse(
                    description = "if no query results are found",
                    responseCode = "204",
                    content = @Content(schema = @Schema(hidden = true))),
            @ApiResponse(
                    description = "if parameter validation fails<br>" +
                            "if query logic parameter validation fails<br>" +
                            "if security marking validation fails<br>" +
                            "if auditing fails",
                    responseCode = "400",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the user doesn't have access to the requested query logic",
                    responseCode = "401",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if query storage fails<br>" +
                            "if the next call times out<br>" +
                            "if the next task is rejected by the executor<br>" +
                            "if there is an unknown error",
                    responseCode = "500",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class)))})
    @Parameters({
            @Parameter(
                    name = QUERY_BEGIN,
                    in = ParameterIn.QUERY,
                    description = "The query begin date",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "\"19660908 000000.000\""),
            @Parameter(
                    name = QUERY_END,
                    in = ParameterIn.QUERY,
                    description = "The query end date",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "\"20161002 235959.999\""),
            @Parameter(
                    name = QUERY_NAME,
                    in = ParameterIn.QUERY,
                    description = "The query name",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "Developer Test Query"),
            @Parameter(
                    name = QUERY_AUTHORIZATIONS,
                    in = ParameterIn.QUERY,
                    description = "The query auths",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "PUBLIC,PRIVATE,BAR,FOO"),
            @Parameter(
                    name = QUERY_VISIBILITY,
                    in = ParameterIn.QUERY,
                    description = "The visibility to use when storing metrics for this query",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "PUBLIC"),
            @Parameter(
                    name = QUERY_MAX_CONCURRENT_TASKS,
                    in = ParameterIn.QUERY,
                    description = "The max number of concurrent tasks to run for this query",
                    schema = @Schema(implementation = Integer.class),
                    example = "10"),
            @Parameter(
                    name = QUERY_POOL,
                    in = ParameterIn.QUERY,
                    description = "The executor pool to run against",
                    schema = @Schema(implementation = String.class),
                    example = "pool1"),
            @Parameter(
                    name = QUERY_PAGESIZE,
                    in = ParameterIn.QUERY,
                    description = "The requested page size",
                    schema = @Schema(implementation = Integer.class),
                    example = "10"),
            @Parameter(
                    name = QUERY_PAGETIMEOUT,
                    in = ParameterIn.QUERY,
                    description = "The call timeout when requesting a page, in minutes",
                    schema = @Schema(implementation = Integer.class),
                    example = "60"),
            @Parameter(
                    name = QUERY_MAX_RESULTS_OVERRIDE,
                    in = ParameterIn.QUERY,
                    description = "The max results override value",
                    schema = @Schema(implementation = Integer.class),
                    example = "5000"),
            @Parameter(
                    name = QUERY_PARAMS,
                    in = ParameterIn.QUERY,
                    description = "Additional query parameters",
                    schema = @Schema(implementation = String.class),
                    example = "KEY_1:VALUE_1;KEY_2:VALUE_2")
    })
    // @formatter:on
    @RequestMapping(path = "translateId/{id}", method = {RequestMethod.GET}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public BaseQueryResponse translateId(@Parameter(description = "The ID to translate") @PathVariable String id,
                    @Parameter(hidden = true) @RequestParam MultiValueMap<String,String> parameters, @RequestHeader HttpHeaders headers,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) throws QueryException {
        return translateIdService.translateId(id, parameters, getPool(headers), currentUser);
    }
    
    // @see TranslateIdService#translateIds(MultiValueMap, String, DatawaveUserDetails)
    // @formatter:off
    @Operation(
            summary = "Get the ID(s), if any, associated with the specified IDs.",
            description = "Because the query created by this call may return multiple pages, callers are expected to request additional pages and eventually close the query.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a base query response containing the first page of results",
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = BaseQueryResponse.class)),
                    headers = {
                            @Header(
                                    name = "Pool",
                                    description = "the executor pool to target",
                                    schema = @Schema(defaultValue = "default"))}),
            @ApiResponse(
                    description = "if no query results are found",
                    responseCode = "204",
                    content = @Content(schema = @Schema(hidden = true))),
            @ApiResponse(
                    description = "if parameter validation fails<br>" +
                            "if query logic parameter validation fails<br>" +
                            "if security marking validation fails<br>" +
                            "if auditing fails",
                    responseCode = "400",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the user doesn't have access to the requested query logic",
                    responseCode = "401",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if query storage fails<br>" +
                            "if the next call times out<br>" +
                            "if the next task is rejected by the executor<br>" +
                            "if there is an unknown error",
                    responseCode = "500",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class)))})
    @Parameters({
            @Parameter(
                    name = TRANSLATE_ID,
                    in = ParameterIn.QUERY,
                    description = "The IDs to translate",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "\"19660908 000000.000\""),
            @Parameter(
                    name = QUERY_BEGIN,
                    in = ParameterIn.QUERY,
                    description = "The query begin date",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "\"19660908 000000.000\""),
            @Parameter(
                    name = QUERY_END,
                    in = ParameterIn.QUERY,
                    description = "The query end date",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "\"20161002 235959.999\""),
            @Parameter(
                    name = QUERY_NAME,
                    in = ParameterIn.QUERY,
                    description = "The query name",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "Developer Test Query"),
            @Parameter(
                    name = QUERY_AUTHORIZATIONS,
                    in = ParameterIn.QUERY,
                    description = "The query auths",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "PUBLIC,PRIVATE,BAR,FOO"),
            @Parameter(
                    name = QUERY_VISIBILITY,
                    in = ParameterIn.QUERY,
                    description = "The visibility to use when storing metrics for this query",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "PUBLIC"),
            @Parameter(
                    name = QUERY_MAX_CONCURRENT_TASKS,
                    in = ParameterIn.QUERY,
                    description = "The max number of concurrent tasks to run for this query",
                    schema = @Schema(implementation = Integer.class),
                    example = "10"),
            @Parameter(
                    name = QUERY_POOL,
                    in = ParameterIn.QUERY,
                    description = "The executor pool to run against",
                    schema = @Schema(implementation = String.class),
                    example = "pool1"),
            @Parameter(
                    name = QUERY_PAGESIZE,
                    in = ParameterIn.QUERY,
                    description = "The requested page size",
                    schema = @Schema(implementation = Integer.class),
                    example = "10"),
            @Parameter(
                    name = QUERY_PAGETIMEOUT,
                    in = ParameterIn.QUERY,
                    description = "The call timeout when requesting a page, in minutes",
                    schema = @Schema(implementation = Integer.class),
                    example = "60"),
            @Parameter(
                    name = QUERY_MAX_RESULTS_OVERRIDE,
                    in = ParameterIn.QUERY,
                    description = "The max results override value",
                    schema = @Schema(implementation = Integer.class),
                    example = "5000"),
            @Parameter(
                    name = QUERY_PARAMS,
                    in = ParameterIn.QUERY,
                    description = "Additional query parameters",
                    schema = @Schema(implementation = String.class),
                    example = "KEY_1:VALUE_1;KEY_2:VALUE_2")
    })
    // @formatter:on
    @RequestMapping(path = "translateIDs", method = {RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public BaseQueryResponse translateIDs(@Parameter(hidden = true) @RequestParam MultiValueMap<String,String> parameters, @RequestHeader HttpHeaders headers,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) throws QueryException {
        return translateIdService.translateIds(parameters, getPool(headers), currentUser);
    }
    
    // @see QueryManagementService#createAndNext(String, MultiValueMap, String, DatawaveUserDetails)
    // @formatter:off
    @Operation(
            summary = "Creates a query using the given query logic and parameters, and returns the first page of results.",
            description = "Created queries will start running immediately.<br>" +
                    "Auditing is performed before the query is started.<br>" +
                    "Subsequent query results can be retrieved using <strong>next</strong>.<br>" +
                    "Updates can be made to any parameter which doesn't affect the scope of the query using <strong>update</strong>.<br>" +
                    "Stop a running query gracefully using <strong>close</strong> or forcefully using <strong>cancel</strong>.<br>" +
                    "Stop, and restart a running query using <strong>reset</strong>.<br>" +
                    "Create a copy of a running query using <strong>duplicate</strong>.<br>" +
                    "Aside from a limited set of admin actions, only the query owner can act on a running query.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a base query response containing the first page of results",
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = BaseQueryResponse.class)),
                    headers = {
                            @Header(
                                    name = "Pool",
                                    description = "the executor pool to target",
                                    schema = @Schema(defaultValue = "default"))}),
            @ApiResponse(
                    description = "if no query results are found",
                    responseCode = "204",
                    content = @Content(schema = @Schema(hidden = true))),
            @ApiResponse(
                    description = "if parameter validation fails<br>" +
                            "if query logic parameter validation fails<br>" +
                            "if security marking validation fails<br>" +
                            "if auditing fails",
                    responseCode = "400",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the user doesn't have access to the requested query logic",
                    responseCode = "401",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if query storage fails<br>" +
                            "if the next call times out<br>" +
                            "if the next task is rejected by the executor<br>" +
                            "if there is an unknown error",
                    responseCode = "500",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class)))})
    @Parameters({
            @Parameter(
                    name = QUERY_BEGIN,
                    in = ParameterIn.QUERY,
                    description = "The query begin date",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "\"19660908 000000.000\""),
            @Parameter(
                    name = QUERY_END,
                    in = ParameterIn.QUERY,
                    description = "The query end date",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "\"20161002 235959.999\""),
            @Parameter(
                    name = QUERY_NAME,
                    in = ParameterIn.QUERY,
                    description = "The query name",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "Developer Test Query"),
            @Parameter(
                    name = QUERY_STRING,
                    in = ParameterIn.QUERY,
                    description = "The query string",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "GENRES:[Action to Western]"),
            @Parameter(
                    name = QUERY_AUTHORIZATIONS,
                    in = ParameterIn.QUERY,
                    description = "The query auths",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "PUBLIC,PRIVATE,BAR,FOO"),
            @Parameter(
                    name = QUERY_VISIBILITY,
                    in = ParameterIn.QUERY,
                    description = "The visibility to use when storing metrics for this query",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "PUBLIC"),
            @Parameter(
                    name = QUERY_SYNTAX,
                    in = ParameterIn.QUERY,
                    description = "The syntax used in the query",
                    schema = @Schema(implementation = String.class),
                    example = "LUCENE"),
            @Parameter(
                    name = QUERY_MAX_CONCURRENT_TASKS,
                    in = ParameterIn.QUERY,
                    description = "The max number of concurrent tasks to run for this query",
                    schema = @Schema(implementation = Integer.class),
                    example = "10"),
            @Parameter(
                    name = QUERY_POOL,
                    in = ParameterIn.QUERY,
                    description = "The executor pool to run against",
                    schema = @Schema(implementation = String.class),
                    example = "pool1"),
            @Parameter(
                    name = QUERY_PAGESIZE,
                    in = ParameterIn.QUERY,
                    description = "The requested page size",
                    schema = @Schema(implementation = Integer.class),
                    example = "10"),
            @Parameter(
                    name = QUERY_PAGETIMEOUT,
                    in = ParameterIn.QUERY,
                    description = "The call timeout when requesting a page, in minutes",
                    schema = @Schema(implementation = Integer.class),
                    example = "60"),
            @Parameter(
                    name = QUERY_MAX_RESULTS_OVERRIDE,
                    in = ParameterIn.QUERY,
                    description = "The max results override value",
                    schema = @Schema(implementation = Integer.class),
                    example = "5000"),
            @Parameter(
                    name = QUERY_PARAMS,
                    in = ParameterIn.QUERY,
                    description = "Additional query parameters",
                    schema = @Schema(implementation = String.class),
                    example = "KEY_1:VALUE_1;KEY_2:VALUE_2")
    })
    // @formatter:on
    @Timed(name = "dw.query.createAndNext", absolute = true)
    @GenerateQuerySessionId(cookieBasePath = "/query/v1/query/")
    @EnrichQueryMetrics(methodType = EnrichQueryMetrics.MethodType.CREATE_AND_NEXT)
    @RequestMapping(path = "{queryLogic}/createAndNext", method = {RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json",
            "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public BaseQueryResponse createAndNext(@Parameter(description = "The query logic", example = "EventQuery") @PathVariable String queryLogic,
                    @Parameter(hidden = true) @RequestParam MultiValueMap<String,String> parameters, @RequestHeader HttpHeaders headers,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) throws QueryException {
        BaseQueryResponse response = queryManagementService.createAndNext(queryLogic, parameters, getPool(headers), currentUser);
        querySessionIdContext.setQueryId(response.getQueryId());
        return response;
    }
    
    // @see QueryManagementService#next(String, DatawaveUserDetails)
    // @formatter:off
    @Operation(
            summary = "Gets the next page of results for the specified query.",
            description = "Next can only be called on a running query.<br>" +
                    "If configuration allows, multiple next calls may be run concurrently for a query.<br>" +
                    "Only the query owner can call next on the specified query.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a base query response containing the next page of results",
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = BaseQueryResponse.class))),
            @ApiResponse(
                    description = "if no query results are found",
                    responseCode = "204",
                    content = @Content(schema = @Schema(hidden = true))),
            @ApiResponse(
                    description = "if the query is not running",
                    responseCode = "400",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the user doesn't own the query",
                    responseCode = "401",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the query cannot be found",
                    responseCode = "404",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if query lock acquisition fails<br>" +
                            "if the next call is interrupted<br>" +
                            "if the query times out<br>" +
                            "if the next task is rejected by the executor<br>" +
                            "if next call execution fails<br>" +
                            "if query logic creation fails<br>" +
                            "if there is an unknown error",
                    responseCode = "500",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class)))})
    // @formatter:on
    @Timed(name = "dw.query.next", absolute = true)
    @EnrichQueryMetrics(methodType = EnrichQueryMetrics.MethodType.NEXT)
    @RequestMapping(path = "{queryId}/next", method = {RequestMethod.GET}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public BaseQueryResponse next(@Parameter(description = "The query ID") @PathVariable String queryId,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) throws QueryException {
        return queryManagementService.next(queryId, currentUser);
    }
    
    // @see QueryManagementService#cancel(String, DatawaveUserDetails)
    // @formatter:off
    @Operation(
            summary = "Cancels the specified query.",
            description = "Cancel can only be called on a running query, or a query that is in the process of closing.<br>" +
                    "Outstanding next calls will be stopped immediately, but will return partial results if applicable.<br>" +
                    "Aside from admins, only the query owner can cancel the specified query.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a void response indicating that the query was canceled",
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the query is not running",
                    responseCode = "400",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the user doesn't own the query",
                    responseCode = "401",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the query cannot be found",
                    responseCode = "404",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if query lock acquisition fails<br>" +
                            "if the cancel call is interrupted<br>" +
                            "if there is an unknown error",
                    responseCode = "500",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class)))})
    // @formatter:on
    @Timed(name = "dw.query.cancel", absolute = true)
    @ClearQuerySessionId
    @RequestMapping(path = "{queryId}/cancel", method = {RequestMethod.PUT, RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json",
            "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public VoidResponse cancel(@Parameter(description = "The query ID") @PathVariable String queryId, @AuthenticationPrincipal DatawaveUserDetails currentUser)
                    throws QueryException {
        return queryManagementService.cancel(queryId, currentUser);
    }
    
    // @see QueryManagementService#adminCancel(String, DatawaveUserDetails)
    // @formatter:off
    @Operation(
            summary = "Cancels the specified query using admin privileges.",
            description = "Cancel can only be called on a running query, or a query that is in the process of closing.<br>" +
                    "Outstanding next calls will be stopped immediately, but will return partial results if applicable.<br>" +
                    "Only admin users should be allowed to call this method.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a void response indicating that the query was canceled",
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the query is not running",
                    responseCode = "400",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the user doesn't own the query",
                    responseCode = "401",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the query cannot be found",
                    responseCode = "404",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if query lock acquisition fails<br>" +
                            "if the cancel call is interrupted<br>" +
                            "if there is an unknown error",
                    responseCode = "500",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class)))})
    // @formatter:on
    @Timed(name = "dw.query.adminCancel", absolute = true)
    @Secured({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "{queryId}/adminCancel", method = {RequestMethod.PUT, RequestMethod.POST}, produces = {"application/xml", "text/xml",
            "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public VoidResponse adminCancel(@Parameter(description = "The query ID") @PathVariable String queryId,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) throws QueryException {
        return queryManagementService.adminCancel(queryId, currentUser);
    }
    
    // @see QueryManagementService#close(String, DatawaveUserDetails)
    // @formatter:off
    @Operation(
            summary = "Closes the specified query.",
            description = "Close can only be called on a running query.<br>" +
                    "Outstanding next calls will be allowed to run until they can return a full page, or they timeout.<br>" +
                    "Aside from admins, only the query owner can close the specified query.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a void response indicating that the query was closed",
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the query is not running",
                    responseCode = "400",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the user doesn't own the query",
                    responseCode = "401",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the query cannot be found",
                    responseCode = "404",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if query lock acquisition fails<br>" +
                            "if the close call is interrupted<br>" +
                            "if there is an unknown error",
                    responseCode = "500",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class)))})
    // @formatter:on
    @Timed(name = "dw.query.close", absolute = true)
    @ClearQuerySessionId
    @RequestMapping(path = "{queryId}/close", method = {RequestMethod.PUT, RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json",
            "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public VoidResponse close(@Parameter(description = "The query ID") @PathVariable String queryId, @AuthenticationPrincipal DatawaveUserDetails currentUser)
                    throws QueryException {
        return queryManagementService.close(queryId, currentUser);
    }
    
    // @see QueryManagementService#adminClose(String, DatawaveUserDetails)
    // @formatter:off
    @Operation(
            summary = "Closes the specified query using admin privileges.",
            description = "Close can only be called on a running query.<br>" +
                    "Outstanding next calls will be allowed to run until they can return a full page, or they timeout.<br>" +
                    "Only admin users should be allowed to call this method.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a void response indicating that the query was closed",
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the query is not running",
                    responseCode = "400",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the user doesn't own the query",
                    responseCode = "401",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the query cannot be found",
                    responseCode = "404",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if query lock acquisition fails<br>" +
                            "if the close call is interrupted<br>" +
                            "if there is an unknown error",
                    responseCode = "500",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class)))})
    // @formatter:on
    @Timed(name = "dw.query.adminClose", absolute = true)
    @Secured({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "{queryId}/adminClose", method = {RequestMethod.PUT, RequestMethod.POST}, produces = {"application/xml", "text/xml",
            "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public VoidResponse adminClose(@Parameter(description = "The query ID") @PathVariable String queryId,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) throws QueryException {
        return queryManagementService.adminClose(queryId, currentUser);
    }
    
    // @see QueryManagementService#reset(String, DatawaveUserDetails)
    // @formatter:off
    @Operation(
            summary = "Stops, and restarts the specified query.",
            description = "Reset can be called on any query, whether it's running or not.<br>" +
                    "If the specified query is still running, it will be canceled. See <strong>cancel</strong>.<br>" +
                    "Reset creates a new, identical query, with a new query id.<br>" +
                    "Reset queries will start running immediately.<br>" +
                    "Auditing is performed before the new query is started.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a generic response containing the new query id",
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = BaseQueryResponse.class))),
            @ApiResponse(
                    description = "if parameter validation fails<br>" +
                            "if query logic parameter validation fails<br>" +
                            "if security marking validation fails<br>" +
                            "if auditing fails",
                    responseCode = "400",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the user doesn't own the query<br>" +
                            "if the user doesn't have access to the requested query logic",
                    responseCode = "401",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the query cannot be found",
                    responseCode = "404",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if query lock acquisition fails<br>" +
                            "if the cancel call is interrupted<br>" +
                            "if query storage fails<br>" +
                            "if there is an unknown error",
                    responseCode = "500",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class)))})
    // @formatter:on
    @Timed(name = "dw.query.reset", absolute = true)
    @GenerateQuerySessionId(cookieBasePath = "/query/v1/query/")
    @RequestMapping(path = "{queryId}/reset", method = {RequestMethod.PUT, RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json",
            "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public GenericResponse<String> reset(@Parameter(description = "The query ID") @PathVariable String queryId,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) throws QueryException {
        GenericResponse<String> response = queryManagementService.reset(queryId, currentUser);
        querySessionIdContext.setQueryId(response.getResult());
        return response;
    }
    
    // @see QueryManagementService#remove(String, DatawaveUserDetails)
    // @formatter:off
    @Operation(
            summary = "Removes the specified query from query storage.",
            description = "Remove can only be called on a query that is not running.<br>" +
                    "Aside from admins, only the query owner can remove the specified query.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a void response indicating that the query was removed",
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the query is running",
                    responseCode = "400",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the user doesn't own the query",
                    responseCode = "401",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the query cannot be found",
                    responseCode = "404",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if there is an unknown error",
                    responseCode = "500",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class)))})
    // @formatter:on
    @Timed(name = "dw.query.remove", absolute = true)
    @RequestMapping(path = "{queryId}/remove", method = {RequestMethod.DELETE}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public VoidResponse remove(@Parameter(description = "The query ID") @PathVariable String queryId, @AuthenticationPrincipal DatawaveUserDetails currentUser)
                    throws QueryException {
        return queryManagementService.remove(queryId, currentUser);
    }
    
    // @see QueryManagementService#adminRemove(String, DatawaveUserDetails)
    // @formatter:off
    @Operation(
            summary = "Removes the specified query from query storage using admin privileges.",
            description = "Remove can only be called on a query that is not running.<br>" +
                    "Only admin users should be allowed to call this method.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a void response indicating that the query was removed",
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the query is running",
                    responseCode = "400",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the query cannot be found",
                    responseCode = "404",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if there is an unknown error",
                    responseCode = "500",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class)))})
    // @formatter:on
    @Timed(name = "dw.query.adminRemove", absolute = true)
    @Secured({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "{queryId}/adminRemove", method = {RequestMethod.DELETE}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public VoidResponse adminRemove(@Parameter(description = "The query ID") @PathVariable String queryId,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) throws QueryException {
        return queryManagementService.adminRemove(queryId, currentUser);
    }
    
    // @see QueryManagementService#update(String, MultiValueMap, DatawaveUserDetails)
    // @formatter:off
    @Operation(
            summary = "Updates the specified query.",
            description = "Update can only be called on a defined, or running query.<br>" +
                    "Auditing is not performed when updating a defined query.<br>" +
                    "No auditable parameters should be updated when updating a running query.<br>" +
                    "Any query parameter can be updated for a defined query.<br>" +
                    "Query parameters which don't affect the scope of the query can be updated for a running query.<br>" +
                    "The list of parameters that can be updated for a running query is configurable.<br>" +
                    "Auditable parameters should never be added to the updatable parameters configuration.<br>" +
                    "Query string, date range, query logic, and auths should never be updated for a running query.<br>" +
                    "Only the query owner can call update on the specified query.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a generic response containing the query id",
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = GenericResponse.class))),
            @ApiResponse(
                    description = "if parameter validation fails<br>" +
                            "if query logic parameter validation fails<br>" +
                            "if security marking validation fails<br>" +
                            "if the query is not defined, or running<br>" +
                            " if no parameters are specified",
                    responseCode = "400",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the user doesn't own the query<br>" +
                            "if the user doesn't have access to the requested query logic",
                    responseCode = "401",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the query cannot be found",
                    responseCode = "404",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if query lock acquisition fails<br>" +
                            "if the update call is interrupted<br>" +
                            "if query storage fails<br>" +
                            "if there is an unknown error",
                    responseCode = "500",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class)))})
    @Parameters({
            @Parameter(
                    name = QUERY_BEGIN,
                    in = ParameterIn.QUERY,
                    description = "The query begin date",
                    schema = @Schema(implementation = String.class),
                    example = "\"19660908 000000.000\""),
            @Parameter(
                    name = QUERY_END,
                    in = ParameterIn.QUERY,
                    description = "The query end date",
                    schema = @Schema(implementation = String.class),
                    example = "\"20161002 235959.999\""),
            @Parameter(
                    name = QUERY_NAME,
                    in = ParameterIn.QUERY,
                    description = "The query name",
                    schema = @Schema(implementation = String.class),
                    example = "Developer Test Query"),
            @Parameter(
                    name = QUERY_STRING,
                    in = ParameterIn.QUERY,
                    description = "The query string",
                    schema = @Schema(implementation = String.class),
                    example = "GENRES:[Action to Western]"),
            @Parameter(
                    name = QUERY_AUTHORIZATIONS,
                    in = ParameterIn.QUERY,
                    description = "The query auths",
                    schema = @Schema(implementation = String.class),
                    example = "PUBLIC,PRIVATE,BAR,FOO"),
            @Parameter(
                    name = QUERY_VISIBILITY,
                    in = ParameterIn.QUERY,
                    description = "The visibility to use when storing metrics for this query",
                    schema = @Schema(implementation = String.class),
                    example = "PUBLIC"),
            @Parameter(
                    name = QUERY_SYNTAX,
                    in = ParameterIn.QUERY,
                    description = "The syntax used in the query",
                    schema = @Schema(implementation = String.class),
                    example = "LUCENE"),
            @Parameter(
                    name = QUERY_MAX_CONCURRENT_TASKS,
                    in = ParameterIn.QUERY,
                    description = "The max number of concurrent tasks to run for this query",
                    schema = @Schema(implementation = Integer.class),
                    example = "10"),
            @Parameter(
                    name = QUERY_POOL,
                    in = ParameterIn.QUERY,
                    description = "The executor pool to run against",
                    schema = @Schema(implementation = String.class),
                    example = "pool1"),
            @Parameter(
                    name = QUERY_PAGESIZE,
                    in = ParameterIn.QUERY,
                    description = "The requested page size",
                    schema = @Schema(implementation = Integer.class),
                    example = "10"),
            @Parameter(
                    name = QUERY_PAGETIMEOUT,
                    in = ParameterIn.QUERY,
                    description = "The call timeout when requesting a page, in minutes",
                    schema = @Schema(implementation = Integer.class),
                    example = "60"),
            @Parameter(
                    name = QUERY_MAX_RESULTS_OVERRIDE,
                    in = ParameterIn.QUERY,
                    description = "The max results override value",
                    schema = @Schema(implementation = Integer.class),
                    example = "5000"),
            @Parameter(
                    name = QUERY_PARAMS,
                    in = ParameterIn.QUERY,
                    description = "Additional query parameters",
                    schema = @Schema(implementation = String.class),
                    example = "KEY_1:VALUE_1;KEY_2:VALUE_2")
    })
    // @formatter:on
    @Timed(name = "dw.query.update", absolute = true)
    @RequestMapping(path = "{queryId}/update", method = {RequestMethod.PUT, RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json",
            "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public GenericResponse<String> update(@Parameter(description = "The query ID") @PathVariable String queryId,
                    @Parameter(hidden = true) @RequestParam MultiValueMap<String,String> parameters, @AuthenticationPrincipal DatawaveUserDetails currentUser)
                    throws QueryException {
        return queryManagementService.update(queryId, parameters, currentUser);
    }
    
    // @see QueryManagementService#duplicate(String, MultiValueMap, DatawaveUserDetails)
    // @formatter:off
    @Operation(
            summary = "Creates a copy of the specified query.",
            description = "Duplicate can be called on any query, whether it's running or not.<br>" +
                    "Duplicate creates a new, identical query, with a new query id.<br>" +
                    "Provided parameter updates will be applied to the new query.<br>" +
                    "Duplicated queries will start running immediately.<br>" +
                    "Auditing is performed before the new query is started.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a generic response containing the query id",
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = GenericResponse.class))),
            @ApiResponse(
                    description = "if parameter validation fails<br>" +
                            "if query logic parameter validation fails<br>" +
                            "if security marking validation fails<br>" +
                            "if auditing fails",
                    responseCode = "400",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the user doesn't own the query<br>" +
                            "if the user doesn't have access to the requested query logic",
                    responseCode = "401",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the query cannot be found",
                    responseCode = "404",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if query lock acquisition fails<br>" +
                            "if query storage fails<br>" +
                            "if there is an unknown error",
                    responseCode = "500",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class)))})
    @Parameters({
            @Parameter(
                    name = QUERY_BEGIN,
                    in = ParameterIn.QUERY,
                    description = "The query begin date",
                    schema = @Schema(implementation = String.class),
                    example = "\"19660908 000000.000\""),
            @Parameter(
                    name = QUERY_END,
                    in = ParameterIn.QUERY,
                    description = "The query end date",
                    schema = @Schema(implementation = String.class),
                    example = "\"20161002 235959.999\""),
            @Parameter(
                    name = QUERY_NAME,
                    in = ParameterIn.QUERY,
                    description = "The query name",
                    schema = @Schema(implementation = String.class),
                    example = "Developer Test Query"),
            @Parameter(
                    name = QUERY_STRING,
                    in = ParameterIn.QUERY,
                    description = "The query string",
                    schema = @Schema(implementation = String.class),
                    example = "GENRES:[Action to Western]"),
            @Parameter(
                    name = QUERY_AUTHORIZATIONS,
                    in = ParameterIn.QUERY,
                    description = "The query auths",
                    schema = @Schema(implementation = String.class),
                    example = "PUBLIC,PRIVATE,BAR,FOO"),
            @Parameter(
                    name = QUERY_VISIBILITY,
                    in = ParameterIn.QUERY,
                    description = "The visibility to use when storing metrics for this query",
                    schema = @Schema(implementation = String.class),
                    example = "PUBLIC"),
            @Parameter(
                    name = QUERY_SYNTAX,
                    in = ParameterIn.QUERY,
                    description = "The syntax used in the query",
                    schema = @Schema(implementation = String.class),
                    example = "LUCENE"),
            @Parameter(
                    name = QUERY_MAX_CONCURRENT_TASKS,
                    in = ParameterIn.QUERY,
                    description = "The max number of concurrent tasks to run for this query",
                    schema = @Schema(implementation = Integer.class),
                    example = "10"),
            @Parameter(
                    name = QUERY_POOL,
                    in = ParameterIn.QUERY,
                    description = "The executor pool to run against",
                    schema = @Schema(implementation = String.class),
                    example = "pool1"),
            @Parameter(
                    name = QUERY_PAGESIZE,
                    in = ParameterIn.QUERY,
                    description = "The requested page size",
                    schema = @Schema(implementation = Integer.class),
                    example = "10"),
            @Parameter(
                    name = QUERY_PAGETIMEOUT,
                    in = ParameterIn.QUERY,
                    description = "The call timeout when requesting a page, in minutes",
                    schema = @Schema(implementation = Integer.class),
                    example = "60"),
            @Parameter(
                    name = QUERY_MAX_RESULTS_OVERRIDE,
                    in = ParameterIn.QUERY,
                    description = "The max results override value",
                    schema = @Schema(implementation = Integer.class),
                    example = "5000"),
            @Parameter(
                    name = QUERY_PARAMS,
                    in = ParameterIn.QUERY,
                    description = "Additional query parameters",
                    schema = @Schema(implementation = String.class),
                    example = "KEY_1:VALUE_1;KEY_2:VALUE_2")
    })
    // @formatter:on
    @Timed(name = "dw.query.duplicate", absolute = true)
    @RequestMapping(path = "{queryId}/duplicate", method = {RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public GenericResponse<String> duplicate(@Parameter(description = "The query ID") @PathVariable String queryId,
                    @Parameter(hidden = true) @RequestParam MultiValueMap<String,String> parameters, @AuthenticationPrincipal DatawaveUserDetails currentUser)
                    throws QueryException {
        return queryManagementService.duplicate(queryId, parameters, currentUser);
    }
    
    // @see QueryManagementService#list(String, String, DatawaveUserDetails)
    // @formatter:off
    @Operation(
            summary = "Gets a list of queries for the calling user.",
            description = "Returns all matching queries owned by the calling user, filtering by query id and query name.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a list response containing the matching queries",
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = QueryImplListResponse.class))),
            @ApiResponse(
                    description = "if there is an unknown error",
                    responseCode = "500",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class)))})
    // @formatter:on
    @Timed(name = "dw.query.list", absolute = true)
    @RequestMapping(path = "list", method = {RequestMethod.GET}, produces = {"text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml",
            "application/x-protobuf", "application/x-protostuff"})
    public QueryImplListResponse list(@Parameter(description = "The query ID") @RequestParam(required = false) String queryId,
                    @Parameter(description = "The query name") @RequestParam(required = false) String queryName,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) throws QueryException {
        return queryManagementService.list(queryId, queryName, currentUser);
    }
    
    // @see QueryManagementService#adminList(String, String, String, DatawaveUserDetails)
    // @formatter:off
    @Operation(
            summary = "Gets a list of queries for the specified user using admin privileges.",
            description = "Returns all matching queries owned by any user, filtered by user ID, query ID, and query name.<br>" +
                    "Only admin users should be allowed to call this method.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a list response containing the matching queries",
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = QueryImplListResponse.class))),
            @ApiResponse(
                    description = "if there is an unknown error",
                    responseCode = "500",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class)))})
    // @formatter:on
    @Timed(name = "dw.query.adminList", absolute = true)
    @Secured({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "adminList", method = {RequestMethod.GET}, produces = {"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml",
            "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public QueryImplListResponse adminList(@Parameter(description = "The query ID") @RequestParam(required = false) String queryId,
                    @Parameter(description = "The user id") @RequestParam(required = false) String user,
                    @Parameter(description = "The query name") @RequestParam(required = false) String queryName,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) throws QueryException {
        return queryManagementService.adminList(queryId, queryName, user, currentUser);
    }
    
    // @see QueryManagementService#list(String, String, DatawaveUserDetails)
    // @formatter:off
    @Operation(
            summary = "Gets the matching query for the calling user.",
            description = "Returns all matching queries owned by the calling user, filtering by query id.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a list response containing the matching query",
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = QueryImplListResponse.class))),
            @ApiResponse(
                    description = "if there is an unknown error",
                    responseCode = "500",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class)))})
    // @formatter:on
    @Timed(name = "dw.query.get", absolute = true)
    @RequestMapping(path = "{queryId}", method = {RequestMethod.GET}, produces = {"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml",
            "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public QueryImplListResponse get(@Parameter(description = "The query ID") @PathVariable String queryId,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) throws QueryException {
        return queryManagementService.list(queryId, null, currentUser);
    }
    
    // @see QueryManagementService#plan(String, DatawaveUserDetails)
    // @formatter:off
    @Operation(
            summary = "Gets the plan for the given query for the calling user.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns the query plan for the matching query",
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = GenericResponse.class))),
            @ApiResponse(
                    description = "if the user doesn't own the query",
                    responseCode = "401",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the query cannot be found",
                    responseCode = "404",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if query lock acquisition fails<br>" +
                            "if query storage fails<br>" +
                            "if there is an unknown error",
                    responseCode = "500",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class)))})
    // @formatter:on
    @Timed(name = "dw.query.plan", absolute = true)
    @RequestMapping(path = "{queryId}/plan", method = {RequestMethod.GET}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public GenericResponse<String> plan(@Parameter(description = "The query ID") @PathVariable String queryId,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) throws QueryException {
        return queryManagementService.plan(queryId, currentUser);
    }
    
    // @see QueryManagementService#predictions(String, DatawaveUserDetails)
    // @formatter:off
    @Operation(
            summary = "Gets the predictions for the given query for the calling user.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns the query predictions for the matching query",
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = GenericResponse.class))),
            @ApiResponse(
                    description = "if the user doesn't own the query",
                    responseCode = "401",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the query cannot be found",
                    responseCode = "404",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if query lock acquisition fails<br>" +
                            "if query storage fails<br>" +
                            "if there is an unknown error",
                    responseCode = "500",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class)))})
    // @formatter:on
    @Timed(name = "dw.query.predictions", absolute = true)
    @RequestMapping(path = "{queryId}/predictions", method = {RequestMethod.GET}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public GenericResponse<String> predictions(@Parameter(description = "The query ID") @PathVariable String queryId,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) throws QueryException {
        return queryManagementService.predictions(queryId, currentUser);
    }
    
    // @see QueryManagementService#adminCancelAll(DatawaveUserDetails)
    // @formatter:off
    @Operation(
            summary = "Cancels all queries using admin privileges.",
            description = "Cancel can only be called on a running query, or a query that is in the process of closing.<br>" +
                    "Queries that are not running will be ignored by this method.<br>" +
                    "Outstanding next calls will be stopped immediately, but will return partial results if applicable.<br>" +
                    "Only admin users should be allowed to call this method.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a void response specifying which queries were canceled",
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if a query cannot be found",
                    responseCode = "404",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if query lock acquisition fails<br>" +
                            "if the cancel call is interrupted<br>" +
                            "if there is an unknown error",
                    responseCode = "500",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class)))})
    // @formatter:on
    @Timed(name = "dw.query.adminCancelAll", absolute = true)
    @Secured({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "adminCancelAll", method = {RequestMethod.PUT, RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json",
            "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public VoidResponse adminCancelAll(@AuthenticationPrincipal DatawaveUserDetails currentUser) throws QueryException {
        return queryManagementService.adminCancelAll(currentUser);
    }
    
    // @see QueryManagementService#adminCloseAll(DatawaveUserDetails)
    // @formatter:off
    @Operation(
            summary = "Closes all queries using admin privileges.",
            description = "Close can only be called on a running query.<br>" +
                    "Queries that are not running will be ignored by this method.<br>" +
                    "Outstanding next calls will be allowed to run until they can return a full page, or they timeout.<br>" +
                    "Only admin users should be allowed to call this method.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a void response specifying which queries were closed",
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if a query cannot be found",
                    responseCode = "404",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if query lock acquisition fails<br>" +
                            "if the close call is interrupted<br>" +
                            "if there is an unknown error",
                    responseCode = "500",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class)))})
    // @formatter:on
    @Timed(name = "dw.query.adminCloseAll", absolute = true)
    @Secured({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "adminCloseAll", method = {RequestMethod.PUT, RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json",
            "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public VoidResponse adminCloseAll(@AuthenticationPrincipal DatawaveUserDetails currentUser) throws QueryException {
        return queryManagementService.adminCloseAll(currentUser);
    }
    
    // @see QueryManagementService#adminRemoveAll(DatawaveUserDetails)
    // @formatter:off
    @Operation(
            summary = "Removes all queries from query storage using admin privileges.",
            description = "Remove can only be called on a query that is not running.<br>" +
                    "Queries that are running will be ignored by this method.<br>" +
                    "Only admin users should be allowed to call this method.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a void response specifying which queries were removed",
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if a query cannot be found",
                    responseCode = "404",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if there is an unknown error",
                    responseCode = "500",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class)))})
    // @formatter:on
    @Timed(name = "dw.query.adminRemoveAll", absolute = true)
    @Secured({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "adminRemoveAll", method = {RequestMethod.DELETE}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public VoidResponse adminRemoveAll(@AuthenticationPrincipal DatawaveUserDetails currentUser) throws QueryException {
        return queryManagementService.adminRemoveAll(currentUser);
    }
    
    // @see StreamingService#createAndExecute(String, MultiValueMap, String, DatawaveUserDetails, DatawaveUserDetails, StreamingResponseListener)
    // @formatter:off
    @Operation(
            summary = "Creates a query using the given query logic and parameters, and streams back all pages of results.",
            description = "Created queries will start running immediately.<br>" +
                    "Auditing is performed before the query is started.<br>" +
                    "Stop a running query gracefully using <strong>close</strong> or forcefully using <strong>cancel</strong>.<br>" +
                    "Stop, and restart a running query using <strong>reset</strong>.<br>" +
                    "Create a copy of a running query using <strong>duplicate</strong>.<br>" +
                    "Aside from a limited set of admin actions, only the query owner can act on a running query.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns multiple base query responses containing pages of results",
                    responseCode = "200",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = BaseQueryResponse.class))),
                    headers = {
                            @Header(
                                    name = "Pool",
                                    description = "the executor pool to target",
                                    schema = @Schema(defaultValue = "default"))}),
            @ApiResponse(
                    description = "if no query results are found",
                    responseCode = "204",
                    content = @Content(schema = @Schema(hidden = true))),
            @ApiResponse(
                    description = "if parameter validation fails<br>" +
                            "if query logic parameter validation fails<br>" +
                            "if security marking validation fails<br>" +
                            "if auditing fails",
                    responseCode = "400",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the user doesn't have access to the requested query logic",
                    responseCode = "401",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if query storage fails<br>" +
                            "if there is an unknown error",
                    responseCode = "500",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class)))})
    @Parameters({
            @Parameter(
                    name = QUERY_BEGIN,
                    in = ParameterIn.QUERY,
                    description = "The query begin date",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "\"19660908 000000.000\""),
            @Parameter(
                    name = QUERY_END,
                    in = ParameterIn.QUERY,
                    description = "The query end date",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "\"20161002 235959.999\""),
            @Parameter(
                    name = QUERY_NAME,
                    in = ParameterIn.QUERY,
                    description = "The query name",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "Developer Test Query"),
            @Parameter(
                    name = QUERY_STRING,
                    in = ParameterIn.QUERY,
                    description = "The query string",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "GENRES:[Action to Western]"),
            @Parameter(
                    name = QUERY_AUTHORIZATIONS,
                    in = ParameterIn.QUERY,
                    description = "The query auths",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "PUBLIC,PRIVATE,BAR,FOO"),
            @Parameter(
                    name = QUERY_VISIBILITY,
                    in = ParameterIn.QUERY,
                    description = "The visibility to use when storing metrics for this query",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "PUBLIC"),
            @Parameter(
                    name = QUERY_SYNTAX,
                    in = ParameterIn.QUERY,
                    description = "The syntax used in the query",
                    schema = @Schema(implementation = String.class),
                    example = "LUCENE"),
            @Parameter(
                    name = QUERY_MAX_CONCURRENT_TASKS,
                    in = ParameterIn.QUERY,
                    description = "The max number of concurrent tasks to run for this query",
                    schema = @Schema(implementation = Integer.class),
                    example = "10"),
            @Parameter(
                    name = QUERY_POOL,
                    in = ParameterIn.QUERY,
                    description = "The executor pool to run against",
                    schema = @Schema(implementation = String.class),
                    example = "pool1"),
            @Parameter(
                    name = QUERY_PAGESIZE,
                    in = ParameterIn.QUERY,
                    description = "The requested page size",
                    schema = @Schema(implementation = Integer.class),
                    example = "10"),
            @Parameter(
                    name = QUERY_PAGETIMEOUT,
                    in = ParameterIn.QUERY,
                    description = "The call timeout when requesting a page, in minutes",
                    schema = @Schema(implementation = Integer.class),
                    example = "60"),
            @Parameter(
                    name = QUERY_MAX_RESULTS_OVERRIDE,
                    in = ParameterIn.QUERY,
                    description = "The max results override value",
                    schema = @Schema(implementation = Integer.class),
                    example = "5000"),
            @Parameter(
                    name = QUERY_PARAMS,
                    in = ParameterIn.QUERY,
                    description = "Additional query parameters",
                    schema = @Schema(implementation = String.class),
                    example = "KEY_1:VALUE_1;KEY_2:VALUE_2")
    })
    // @formatter:on
    @Timed(name = "dw.query.createAndExecuteQuery", absolute = true)
    @RequestMapping(path = "{queryLogic}/createAndExecute", method = {RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json",
            "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public ResponseEntity<ResponseBodyEmitter> createAndExecute(
                    @Parameter(description = "The query logic", example = "EventQuery") @PathVariable String queryLogic,
                    @Parameter(hidden = true) @RequestParam MultiValueMap<String,String> parameters, @RequestHeader HttpHeaders headers,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) throws QueryException {
        MediaType contentType = determineContentType(headers.getAccept(), MediaType.parseMediaType(streamingProperties.getDefaultContentType()));
        CountingResponseBodyEmitter emitter = baseMethodStatsContext.createCountingResponseBodyEmitter(streamingProperties.getCallTimeoutMillis());
        String queryId = streamingService.createAndExecute(queryLogic, parameters, getPool(headers), currentUser, serverUserDetailsSupplier.get(),
                        new CountingResponseBodyEmitterListener(emitter, contentType));
        
        // unfortunately this needs to be set manually. ResponseBodyAdvice does not run for streaming endpoints
        queryMetricsEnrichmentContext.setMethodType(EnrichQueryMetrics.MethodType.CREATE);
        queryMetricsEnrichmentContext.setQueryId(queryId);
        
        return createStreamingResponse(emitter, contentType);
    }
    
    // @see StreamingService#execute(String, DatawaveUserDetails, DatawaveUserDetails, StreamingResponseListener)
    // @formatter:off
    @Operation(
            summary = "Gets all pages of results for the given query and streams them back.",
            description = "Execute can only be called on a running query.<br>" +
                    "Only the query owner can call execute on the specified query.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns multiple base query responses containing pages of results",
                    responseCode = "200",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = BaseQueryResponse.class)))),
            @ApiResponse(
                    description = "if no query results are found",
                    responseCode = "204",
                    content = @Content(schema = @Schema(hidden = true))),
            @ApiResponse(
                    description = "if the query is not running",
                    responseCode = "400",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the user doesn't own the query",
                    responseCode = "401",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the query cannot be found",
                    responseCode = "404",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if query lock acquisition fails<br>" +
                            "if the execute call is interrupted<br>" +
                            "if the query times out<br>" +
                            "if the next task is rejected by the executor<br>" +
                            "if next call execution fails<br>" +
                            "if query logic creation fails<br>" +
                            "if there is an unknown error",
                    responseCode = "500",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class)))})
    // @formatter:on
    @Timed(name = "dw.query.executeQuery", absolute = true)
    @RequestMapping(path = "{queryId}/execute", method = {RequestMethod.GET}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public ResponseEntity<ResponseBodyEmitter> execute(@Parameter(description = "The query ID") @PathVariable String queryId,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser, @RequestHeader HttpHeaders headers) {
        MediaType contentType = determineContentType(headers.getAccept(), MediaType.parseMediaType(streamingProperties.getDefaultContentType()));
        CountingResponseBodyEmitter emitter = baseMethodStatsContext.createCountingResponseBodyEmitter(streamingProperties.getCallTimeoutMillis());
        streamingService.execute(queryId, currentUser, serverUserDetailsSupplier.get(), new CountingResponseBodyEmitterListener(emitter, contentType));
        
        return createStreamingResponse(emitter, contentType);
    }
    
    private MediaType determineContentType(List<MediaType> acceptedMediaTypes, MediaType defaultMediaType) {
        MediaType mediaType = null;
        
        if (acceptedMediaTypes != null && !acceptedMediaTypes.isEmpty()) {
            MediaType.sortBySpecificityAndQuality(acceptedMediaTypes);
            mediaType = acceptedMediaTypes.get(0);
        }
        
        if (mediaType == null || MediaType.ALL.equals(mediaType)) {
            mediaType = defaultMediaType;
        }
        
        return mediaType;
    }
    
    private String getPool(HttpHeaders headers) {
        return headers.getFirst(queryProperties.getPoolHeader());
    }
    
    private ResponseEntity<ResponseBodyEmitter> createStreamingResponse(ResponseBodyEmitter emitter, MediaType contentType) {
        HttpHeaders responseHeaders = new HttpHeaders();
        responseHeaders.setContentType(contentType);
        return new ResponseEntity<>(emitter, responseHeaders, HttpStatus.OK);
    }
}
