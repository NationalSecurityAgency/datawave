package datawave.microservice.query;

import com.codahale.metrics.annotation.Timed;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.microservice.query.lookup.LookupService;
import datawave.microservice.query.stream.StreamingService;
import datawave.microservice.query.stream.listener.CountingResponseBodyEmitterListener;
import datawave.microservice.query.web.annotation.EnrichQueryMetrics;
import datawave.microservice.query.web.filter.BaseMethodStatsFilter;
import datawave.microservice.query.web.filter.CountingResponseBodyEmitter;
import datawave.microservice.query.web.filter.QueryMetricsEnrichmentFilterAdvice;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.GenericResponse;
import datawave.webservice.result.QueryImplListResponse;
import datawave.webservice.result.QueryLogicResponse;
import datawave.webservice.result.VoidResponse;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyEmitter;

import javax.annotation.security.RolesAllowed;

@RestController
@RequestMapping(path = "/v1", produces = MediaType.APPLICATION_JSON_VALUE)
public class QueryController {
    private final QueryManagementService queryManagementService;
    private final LookupService lookupService;
    private final StreamingService streamingService;
    
    // Note: baseMethodStatsContext needs to be request scoped
    private final BaseMethodStatsFilter.BaseMethodStatsContext baseMethodStatsContext;
    // Note: queryMetricsEnrichmentContest needs to be request scoped
    private final QueryMetricsEnrichmentFilterAdvice.QueryMetricsEnrichmentContext queryMetricsEnrichmentContext;
    
    public QueryController(QueryManagementService queryManagementService, LookupService lookupService, StreamingService streamingService,
                    BaseMethodStatsFilter.BaseMethodStatsContext baseMethodStatsContext,
                    QueryMetricsEnrichmentFilterAdvice.QueryMetricsEnrichmentContext queryMetricsEnrichmentContext) {
        this.queryManagementService = queryManagementService;
        this.lookupService = lookupService;
        this.streamingService = streamingService;
        this.baseMethodStatsContext = baseMethodStatsContext;
        this.queryMetricsEnrichmentContext = queryMetricsEnrichmentContext;
    }
    
    @Timed(name = "dw.query.listQueryLogic", absolute = true)
    @RequestMapping(path = "listQueryLogic", method = {RequestMethod.GET},
                    produces = {"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "text/html"})
    public QueryLogicResponse listQueryLogic(@AuthenticationPrincipal ProxiedUserDetails currentUser) {
        return queryManagementService.listQueryLogic(currentUser);
    }
    
    @Timed(name = "dw.query.defineQuery", absolute = true)
    @EnrichQueryMetrics(methodType = EnrichQueryMetrics.MethodType.CREATE)
    @RequestMapping(path = "{queryLogic}/define", method = {RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public GenericResponse<String> define(@PathVariable String queryLogic, @RequestParam MultiValueMap<String,String> parameters,
                    @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return queryManagementService.define(queryLogic, parameters, currentUser);
    }
    
    @Timed(name = "dw.query.createQuery", absolute = true)
    @EnrichQueryMetrics(methodType = EnrichQueryMetrics.MethodType.CREATE)
    @RequestMapping(path = "{queryLogic}/create", method = {RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public GenericResponse<String> create(@PathVariable String queryLogic, @RequestParam MultiValueMap<String,String> parameters,
                    @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return queryManagementService.create(queryLogic, parameters, currentUser);
    }
    
    @Timed(name = "dw.query.planQuery", absolute = true)
    @RequestMapping(path = "{queryLogic}/plan", method = {RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public GenericResponse<String> plan(@PathVariable String queryLogic, @RequestParam MultiValueMap<String,String> parameters,
                    @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return queryManagementService.plan(queryLogic, parameters, currentUser);
    }
    
    @Timed(name = "dw.query.predictQuery", absolute = true)
    @RequestMapping(path = "{queryLogic}/predict", method = {RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public GenericResponse<String> predict(@PathVariable String queryLogic, @RequestParam MultiValueMap<String,String> parameters,
                    @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return queryManagementService.predict(queryLogic, parameters, currentUser);
    }
    
    @Timed(name = "dw.query.lookupUUID", absolute = true)
    @RequestMapping(path = "lookupUUID/{uuidType}/{uuid}", method = {RequestMethod.GET}, produces = {"application/xml", "text/xml", "application/json",
            "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public BaseQueryResponse lookupUUID(@PathVariable(required = false) String uuidType, @PathVariable(required = false) String uuid,
                    @RequestParam MultiValueMap<String,String> parameters, @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return lookupService.lookupUUID(uuidType, uuid, parameters, currentUser);
    }
    
    @Timed(name = "dw.query.lookupUUIDBatch", absolute = true)
    @RequestMapping(path = "lookupUUID", method = {RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public BaseQueryResponse lookupUUIDBatch(@PathVariable(required = false) String uuidType, @PathVariable(required = false) String uuid,
                    @RequestParam MultiValueMap<String,String> parameters, @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return lookupService.lookupUUID(parameters, currentUser);
    }
    
    @Timed(name = "dw.query.lookupContentUUID", absolute = true)
    @RequestMapping(path = "lookupContentUUID/{uuidType}/{uuid}", method = {RequestMethod.GET}, produces = {"application/xml", "text/xml", "application/json",
            "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public BaseQueryResponse lookupContentUUID(@PathVariable(required = false) String uuidType, @PathVariable(required = false) String uuid,
                    @RequestParam MultiValueMap<String,String> parameters, @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return lookupService.lookupContentUUID(uuidType, uuid, parameters, currentUser);
    }
    
    @Timed(name = "dw.query.lookupContentUUIDBatch", absolute = true)
    @RequestMapping(path = "lookupContentUUID", method = {RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public BaseQueryResponse lookupContentUUIDBatch(@PathVariable(required = false) String uuidType, @PathVariable(required = false) String uuid,
                    @RequestParam MultiValueMap<String,String> parameters, @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return lookupService.lookupContentUUID(parameters, currentUser);
    }
    
    @Timed(name = "dw.query.createAndNext", absolute = true)
    @EnrichQueryMetrics(methodType = EnrichQueryMetrics.MethodType.CREATE_AND_NEXT)
    @RequestMapping(path = "{queryLogic}/createAndNext", method = {RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json",
            "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public BaseQueryResponse createAndNext(@PathVariable String queryLogic, @RequestParam MultiValueMap<String,String> parameters,
                    @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return queryManagementService.createAndNext(queryLogic, parameters, currentUser);
    }
    
    @Timed(name = "dw.query.next", absolute = true)
    @EnrichQueryMetrics(methodType = EnrichQueryMetrics.MethodType.NEXT)
    @RequestMapping(path = "{queryId}/next", method = {RequestMethod.GET}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public BaseQueryResponse next(@PathVariable String queryId, @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return queryManagementService.next(queryId, currentUser);
    }
    
    @Timed(name = "dw.query.cancel", absolute = true)
    @RequestMapping(path = "{queryId}/cancel", method = {RequestMethod.PUT, RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json",
            "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public VoidResponse cancel(@PathVariable String queryId, @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return queryManagementService.cancel(queryId, currentUser);
    }
    
    @Timed(name = "dw.query.adminCancel", absolute = true)
    @RolesAllowed({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "{queryId}/adminCancel", method = {RequestMethod.PUT, RequestMethod.POST}, produces = {"application/xml", "text/xml",
            "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public VoidResponse adminCancel(@PathVariable String queryId, @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return queryManagementService.adminCancel(queryId, currentUser);
    }
    
    @Timed(name = "dw.query.close", absolute = true)
    @RequestMapping(path = "{queryId}/close", method = {RequestMethod.PUT, RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json",
            "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public VoidResponse close(@PathVariable String queryId, @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return queryManagementService.close(queryId, currentUser);
    }
    
    @Timed(name = "dw.query.adminClose", absolute = true)
    @RolesAllowed({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "{queryId}/adminClose", method = {RequestMethod.PUT, RequestMethod.POST}, produces = {"application/xml", "text/xml",
            "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public VoidResponse adminClose(@PathVariable String queryId, @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return queryManagementService.adminClose(queryId, currentUser);
    }
    
    @Timed(name = "dw.query.reset", absolute = true)
    @RequestMapping(path = "{queryId}/reset", method = {RequestMethod.PUT, RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json",
            "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public GenericResponse<String> reset(@PathVariable String queryId, @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return queryManagementService.reset(queryId, currentUser);
    }
    
    @Timed(name = "dw.query.remove", absolute = true)
    @RequestMapping(path = "{queryId}/remove", method = {RequestMethod.DELETE}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public VoidResponse remove(@PathVariable String queryId, @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return queryManagementService.remove(queryId, currentUser);
    }
    
    @Timed(name = "dw.query.adminRemove", absolute = true)
    @RolesAllowed({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "{queryId}/adminRemove", method = {RequestMethod.DELETE}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public VoidResponse adminRemove(@PathVariable String queryId, @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return queryManagementService.adminRemove(queryId, currentUser);
    }
    
    @Timed(name = "dw.query.update", absolute = true)
    @RequestMapping(path = "{queryId}/update", method = {RequestMethod.PUT, RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json",
            "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public GenericResponse<String> update(@PathVariable String queryId, @RequestParam MultiValueMap<String,String> parameters,
                    @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return queryManagementService.update(queryId, parameters, currentUser);
    }
    
    @Timed(name = "dw.query.duplicate", absolute = true)
    @RequestMapping(path = "{queryId}/duplicate", method = {RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public GenericResponse<String> duplicate(@PathVariable String queryId, @RequestParam MultiValueMap<String,String> parameters,
                    @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return queryManagementService.duplicate(queryId, parameters, currentUser);
    }
    
    @Timed(name = "dw.query.list", absolute = true)
    @RequestMapping(path = "list", method = {RequestMethod.GET}, produces = {"text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml",
            "application/x-protobuf", "application/x-protostuff"})
    public QueryImplListResponse list(@RequestParam(required = false) String queryId, @RequestParam(required = false) String queryName,
                    @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return queryManagementService.list(queryId, queryName, currentUser);
    }
    
    @Timed(name = "dw.query.adminList", absolute = true)
    @RolesAllowed({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "adminList", method = {RequestMethod.GET}, produces = {"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml",
            "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public QueryImplListResponse adminList(@RequestParam(required = false) String queryId, @RequestParam(required = false) String user,
                    @RequestParam(required = false) String queryName, @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return queryManagementService.adminList(queryId, queryName, user, currentUser);
    }
    
    @Timed(name = "dw.query.get", absolute = true)
    @RequestMapping(path = "{queryId}", method = {RequestMethod.GET}, produces = {"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml",
            "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public QueryImplListResponse get(@PathVariable String queryId, @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return queryManagementService.list(queryId, null, currentUser);
    }
    
    @Timed(name = "dw.query.plan", absolute = true)
    @RequestMapping(path = "{queryId}/plan", method = {RequestMethod.GET}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public GenericResponse<String> plan(@PathVariable String queryId, @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return queryManagementService.plan(queryId, currentUser);
    }
    
    @Timed(name = "dw.query.predictions", absolute = true)
    @RequestMapping(path = "{queryId}/predictions", method = {RequestMethod.GET}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public GenericResponse<String> predictions(@PathVariable String queryId, @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return queryManagementService.predictions(queryId, currentUser);
    }
    
    @Timed(name = "dw.query.adminCancelAll", absolute = true)
    @RolesAllowed({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "adminCancelAll", method = {RequestMethod.PUT, RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json",
            "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public VoidResponse adminCancelAll(@AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return queryManagementService.adminCancelAll(currentUser);
    }
    
    @Timed(name = "dw.query.adminCloseAll", absolute = true)
    @RolesAllowed({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "adminCloseAll", method = {RequestMethod.PUT, RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json",
            "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public VoidResponse adminCloseAll(@AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return queryManagementService.adminCloseAll(currentUser);
    }
    
    @Timed(name = "dw.query.adminRemoveAll", absolute = true)
    @RolesAllowed({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "adminRemoveAll", method = {RequestMethod.DELETE}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public VoidResponse adminRemoveAll(@AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return queryManagementService.adminRemoveAll(currentUser);
    }
    
    @Timed(name = "dw.query.executeQuery", absolute = true)
    @RequestMapping(path = "{queryLogic}/execute", method = {RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public ResponseBodyEmitter execute(@PathVariable String queryLogic, @RequestParam MultiValueMap<String,String> parameters,
                    @AuthenticationPrincipal ProxiedUserDetails currentUser, @RequestHeader HttpHeaders headers) throws QueryException {
        String queryId = queryManagementService.create(queryLogic, parameters, currentUser).getResult();
        
        // unfortunately this needs to be set manually. ResponseBodyAdvice does not run for streaming endpoints
        queryMetricsEnrichmentContext.setMethodType(EnrichQueryMetrics.MethodType.CREATE);
        queryMetricsEnrichmentContext.setQueryId(queryId);
        
        CountingResponseBodyEmitter emitter = baseMethodStatsContext.getCountingResponseBodyEmitter();
        
        // bytesWritten is updated as streaming data is written to the response output stream
        streamingService.execute(queryId, parameters, currentUser, new CountingResponseBodyEmitterListener(emitter, headers.getAccept()));
        
        return emitter;
    }
}
