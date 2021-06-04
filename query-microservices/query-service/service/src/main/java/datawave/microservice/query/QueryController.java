package datawave.microservice.query;

import com.codahale.metrics.annotation.Timed;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.microservice.common.storage.TaskKey;
import datawave.microservice.query.web.annotation.EnrichQueryMetrics;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.GenericResponse;
import datawave.webservice.result.VoidResponse;
import org.springframework.http.MediaType;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(path = "/v1", produces = MediaType.APPLICATION_JSON_VALUE)
public class QueryController {
    private final QueryManagementService queryManagementService;
    
    public QueryController(QueryManagementService queryManagementService) {
        this.queryManagementService = queryManagementService;
    }
    
    @Timed(name = "dw.query.defineQuery", absolute = true)
    @EnrichQueryMetrics(methodType = EnrichQueryMetrics.MethodType.CREATE)
    @RequestMapping(path = "{queryLogic}/define", method = {RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public GenericResponse<String> define(@PathVariable(name = "queryLogic") String queryLogic, @RequestParam MultiValueMap<String,String> parameters,
                    @AuthenticationPrincipal ProxiedUserDetails currentUser) throws Exception {
        TaskKey taskKey = queryManagementService.define(queryLogic, parameters, currentUser);
        
        GenericResponse<String> resp = new GenericResponse<>();
        resp.setResult(taskKey.getQueryId().toString());
        return resp;
    }
    
    @Timed(name = "dw.query.createQuery", absolute = true)
    @EnrichQueryMetrics(methodType = EnrichQueryMetrics.MethodType.CREATE)
    @RequestMapping(path = "{queryLogic}/create", method = {RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public GenericResponse<String> create(@PathVariable(name = "queryLogic") String queryLogic, @RequestParam MultiValueMap<String,String> parameters,
                    @AuthenticationPrincipal ProxiedUserDetails currentUser) throws Exception {
        TaskKey taskKey = queryManagementService.create(queryLogic, parameters, currentUser);
        
        GenericResponse<String> resp = new GenericResponse<>();
        resp.setHasResults(true);
        resp.setResult(taskKey.getQueryId().toString());
        return resp;
    }
    
    @Timed(name = "dw.query.createAndNext", absolute = true)
    @EnrichQueryMetrics(methodType = EnrichQueryMetrics.MethodType.CREATE_AND_NEXT)
    @RequestMapping(path = "{queryLogic}/createAndNext", method = {RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json",
            "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public BaseQueryResponse createQueryAndNext(@PathVariable(name = "queryLogic") String queryLogic, @RequestParam MultiValueMap<String,String> parameters,
                    @AuthenticationPrincipal ProxiedUserDetails currentUser) throws Exception {
        return queryManagementService.createAndNext(queryLogic, parameters, currentUser);
    }
    
    @Timed(name = "dw.query.next", absolute = true)
    @EnrichQueryMetrics(methodType = EnrichQueryMetrics.MethodType.NEXT)
    @RequestMapping(path = "{queryId}/next", method = {RequestMethod.GET}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public BaseQueryResponse next(@PathVariable(name = "queryId") String queryId, @AuthenticationPrincipal ProxiedUserDetails currentUser) throws Exception {
        return queryManagementService.next(queryId, currentUser);
    }
    
    @Timed(name = "dw.query.cancel", absolute = true)
    @RequestMapping(path = "{queryId}/cancel", method = {RequestMethod.PUT, RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json",
            "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public VoidResponse cancel(@PathVariable(name = "queryId") String queryId, @AuthenticationPrincipal ProxiedUserDetails currentUser) throws Exception {
        queryManagementService.cancel(queryId, currentUser);
        return null;
    }
    
    @Timed(name = "dw.query.close", absolute = true)
    @RequestMapping(path = "{queryId}/close", method = {RequestMethod.PUT, RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json",
            "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public VoidResponse close(@PathVariable(name = "queryId") String queryId, @AuthenticationPrincipal ProxiedUserDetails currentUser) throws Exception {
        queryManagementService.close(queryId, currentUser);
        return null;
    }
    
}
