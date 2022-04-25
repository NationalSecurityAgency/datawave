package datawave.microservice.query;

import com.codahale.metrics.annotation.Timed;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.microservice.query.lookup.LookupService;
import datawave.microservice.query.stream.StreamingProperties;
import datawave.microservice.query.stream.StreamingService;
import datawave.microservice.query.stream.listener.CountingResponseBodyEmitterListener;
import datawave.microservice.query.translateid.TranslateIdService;
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

import java.util.List;

import static datawave.microservice.query.lookup.LookupService.LOOKUP_STREAMING;
import static datawave.microservice.query.lookup.LookupService.LOOKUP_UUID_PAIRS;
import static datawave.services.query.logic.lookup.LookupQueryLogic.LOOKUP_KEY_VALUE_DELIMITER;

@RestController
@RequestMapping(path = "/v1", produces = MediaType.APPLICATION_JSON_VALUE)
public class QueryController {
    private final QueryManagementService queryManagementService;
    private final LookupService lookupService;
    private final StreamingService streamingService;
    private final TranslateIdService translateIdService;
    
    private final StreamingProperties streamingProperties;
    
    // Note: baseMethodStatsContext needs to be request scoped
    private final BaseMethodStatsFilter.BaseMethodStatsContext baseMethodStatsContext;
    // Note: queryMetricsEnrichmentContest needs to be request scoped
    private final QueryMetricsEnrichmentFilterAdvice.QueryMetricsEnrichmentContext queryMetricsEnrichmentContext;
    
    public QueryController(QueryManagementService queryManagementService, LookupService lookupService, StreamingService streamingService,
                    TranslateIdService translateIdService, StreamingProperties streamingProperties,
                    BaseMethodStatsFilter.BaseMethodStatsContext baseMethodStatsContext,
                    QueryMetricsEnrichmentFilterAdvice.QueryMetricsEnrichmentContext queryMetricsEnrichmentContext) {
        this.queryManagementService = queryManagementService;
        this.lookupService = lookupService;
        this.streamingService = streamingService;
        this.translateIdService = translateIdService;
        this.streamingProperties = streamingProperties;
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
    public Object lookupUUID(@PathVariable(required = false) String uuidType, @PathVariable(required = false) String uuid,
                    @RequestParam MultiValueMap<String,String> parameters, @AuthenticationPrincipal ProxiedUserDetails currentUser,
                    @RequestHeader HttpHeaders headers) throws QueryException {
        parameters.add(LOOKUP_UUID_PAIRS, String.join(LOOKUP_KEY_VALUE_DELIMITER, uuidType, uuid));
        
        if (Boolean.parseBoolean(parameters.getFirst(LOOKUP_STREAMING))) {
            MediaType contentType = determineContentType(headers.getAccept(), MediaType.parseMediaType(streamingProperties.getDefaultContentType()));
            CountingResponseBodyEmitter emitter = baseMethodStatsContext.createCountingResponseBodyEmitter(streamingProperties.getCallTimeoutMillis());
            lookupService.lookupUUID(parameters, currentUser, new CountingResponseBodyEmitterListener(emitter, contentType));
            return emitter;
        } else {
            return lookupService.lookupUUID(parameters, currentUser);
        }
    }
    
    @Timed(name = "dw.query.lookupUUIDBatch", absolute = true)
    @RequestMapping(path = "lookupUUID", method = {RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public Object lookupUUIDBatch(@RequestParam MultiValueMap<String,String> parameters, @AuthenticationPrincipal ProxiedUserDetails currentUser,
                    @RequestHeader HttpHeaders headers) throws QueryException {
        if (Boolean.parseBoolean(parameters.getFirst(LOOKUP_STREAMING))) {
            MediaType contentType = determineContentType(headers.getAccept(), MediaType.parseMediaType(streamingProperties.getDefaultContentType()));
            CountingResponseBodyEmitter emitter = baseMethodStatsContext.createCountingResponseBodyEmitter(streamingProperties.getCallTimeoutMillis());
            lookupService.lookupUUID(parameters, currentUser, new CountingResponseBodyEmitterListener(emitter, contentType));
            return emitter;
        } else {
            return lookupService.lookupUUID(parameters, currentUser);
        }
    }
    
    @Timed(name = "dw.query.lookupContentUUID", absolute = true)
    @RequestMapping(path = "lookupContentUUID/{uuidType}/{uuid}", method = {RequestMethod.GET}, produces = {"application/xml", "text/xml", "application/json",
            "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public Object lookupContentUUID(@PathVariable(required = false) String uuidType, @PathVariable(required = false) String uuid,
                    @RequestParam MultiValueMap<String,String> parameters, @AuthenticationPrincipal ProxiedUserDetails currentUser,
                    @RequestHeader HttpHeaders headers) throws QueryException {
        parameters.add(LOOKUP_UUID_PAIRS, String.join(LOOKUP_KEY_VALUE_DELIMITER, uuidType, uuid));
        
        if (Boolean.parseBoolean(parameters.getFirst(LOOKUP_STREAMING))) {
            MediaType contentType = determineContentType(headers.getAccept(), MediaType.parseMediaType(streamingProperties.getDefaultContentType()));
            CountingResponseBodyEmitter emitter = baseMethodStatsContext.createCountingResponseBodyEmitter(streamingProperties.getCallTimeoutMillis());
            lookupService.lookupContentUUID(parameters, currentUser, new CountingResponseBodyEmitterListener(emitter, contentType));
            return emitter;
        } else {
            return lookupService.lookupContentUUID(parameters, currentUser);
        }
    }
    
    @Timed(name = "dw.query.lookupContentUUIDBatch", absolute = true)
    @RequestMapping(path = "lookupContentUUID", method = {RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public Object lookupContentUUIDBatch(@RequestParam MultiValueMap<String,String> parameters, @AuthenticationPrincipal ProxiedUserDetails currentUser,
                    @RequestHeader HttpHeaders headers) throws QueryException {
        if (Boolean.parseBoolean(parameters.getFirst(LOOKUP_STREAMING))) {
            MediaType contentType = determineContentType(headers.getAccept(), MediaType.parseMediaType(streamingProperties.getDefaultContentType()));
            CountingResponseBodyEmitter emitter = baseMethodStatsContext.createCountingResponseBodyEmitter(streamingProperties.getCallTimeoutMillis());
            lookupService.lookupContentUUID(parameters, currentUser, new CountingResponseBodyEmitterListener(emitter, contentType));
            return emitter;
        } else {
            return lookupService.lookupContentUUID(parameters, currentUser);
        }
    }
    
    @RequestMapping(path = "translateId/{id}", method = {RequestMethod.GET}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public BaseQueryResponse translateId(@PathVariable String id, @RequestParam MultiValueMap<String,String> parameters,
                    @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return translateIdService.translateId(id, parameters, currentUser);
    }
    
    // TODO: Shouldn't the case for this path be the same as the singular call?
    @RequestMapping(path = "translateIDs", method = {RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public BaseQueryResponse translateIDs(@RequestParam MultiValueMap<String,String> parameters, @AuthenticationPrincipal ProxiedUserDetails currentUser)
                    throws QueryException {
        return translateIdService.translateIds(parameters, currentUser);
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
    @Secured({"Administrator", "JBossAdministrator"})
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
    @Secured({"Administrator", "JBossAdministrator"})
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
    @Secured({"Administrator", "JBossAdministrator"})
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
    @Secured({"Administrator", "JBossAdministrator"})
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
    @Secured({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "adminCancelAll", method = {RequestMethod.PUT, RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json",
            "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public VoidResponse adminCancelAll(@AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return queryManagementService.adminCancelAll(currentUser);
    }
    
    @Timed(name = "dw.query.adminCloseAll", absolute = true)
    @Secured({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "adminCloseAll", method = {RequestMethod.PUT, RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json",
            "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public VoidResponse adminCloseAll(@AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return queryManagementService.adminCloseAll(currentUser);
    }
    
    @Timed(name = "dw.query.adminRemoveAll", absolute = true)
    @Secured({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "adminRemoveAll", method = {RequestMethod.DELETE}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public VoidResponse adminRemoveAll(@AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return queryManagementService.adminRemoveAll(currentUser);
    }
    
    @Timed(name = "dw.query.createAndExecuteQuery", absolute = true)
    @RequestMapping(path = "{queryLogic}/createAndExecute", method = {RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json",
            "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public ResponseEntity<ResponseBodyEmitter> createAndExecute(@PathVariable String queryLogic, @RequestParam MultiValueMap<String,String> parameters,
                    @AuthenticationPrincipal ProxiedUserDetails currentUser, @RequestHeader HttpHeaders headers) throws QueryException {
        MediaType contentType = determineContentType(headers.getAccept(), MediaType.parseMediaType(streamingProperties.getDefaultContentType()));
        CountingResponseBodyEmitter emitter = baseMethodStatsContext.createCountingResponseBodyEmitter(streamingProperties.getCallTimeoutMillis());
        String queryId = streamingService.createAndExecute(queryLogic, parameters, currentUser, new CountingResponseBodyEmitterListener(emitter, contentType));
        
        // unfortunately this needs to be set manually. ResponseBodyAdvice does not run for streaming endpoints
        queryMetricsEnrichmentContext.setMethodType(EnrichQueryMetrics.MethodType.CREATE);
        queryMetricsEnrichmentContext.setQueryId(queryId);
        
        return createStreamingResponse(emitter, contentType);
    }
    
    @Timed(name = "dw.query.executeQuery", absolute = true)
    @RequestMapping(path = "{queryId}/execute", method = {RequestMethod.GET}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public ResponseEntity<ResponseBodyEmitter> execute(@PathVariable String queryId, @AuthenticationPrincipal ProxiedUserDetails currentUser,
                    @RequestHeader HttpHeaders headers) {
        MediaType contentType = determineContentType(headers.getAccept(), MediaType.parseMediaType(streamingProperties.getDefaultContentType()));
        CountingResponseBodyEmitter emitter = baseMethodStatsContext.createCountingResponseBodyEmitter(streamingProperties.getCallTimeoutMillis());
        streamingService.execute(queryId, currentUser, new CountingResponseBodyEmitterListener(emitter, contentType));
        
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
    
    private ResponseEntity<ResponseBodyEmitter> createStreamingResponse(ResponseBodyEmitter emitter, MediaType contentType) {
        HttpHeaders responseHeaders = new HttpHeaders();
        responseHeaders.setContentType(contentType);
        return new ResponseEntity<>(emitter, responseHeaders, HttpStatus.OK);
    }
}
