package datawave.microservice.query;

import com.codahale.metrics.annotation.Timed;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.microservice.common.storage.TaskKey;
import datawave.microservice.query.web.annotation.EnrichQueryMetrics;
import datawave.webservice.result.GenericResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private QueryManagementService queryManagementService;
    
    public QueryController(QueryManagementService queryManagementService) {
        this.queryManagementService = queryManagementService;
    }
    
    @Timed(name = "dw.query.defineQuery", absolute = true)
    @EnrichQueryMetrics(methodType = EnrichQueryMetrics.MethodType.CREATE)
    @RequestMapping(path = "{queryLogic}/define", method = {RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public GenericResponse<String> define(@PathVariable(name = "queryLogic") String queryLogicName, @RequestParam MultiValueMap<String,String> parameters,
                    @AuthenticationPrincipal ProxiedUserDetails currentUser) throws Exception {
        TaskKey taskKey = queryManagementService.define(queryLogicName, parameters, currentUser);
        
        GenericResponse<String> resp = new GenericResponse<>();
        resp.setResult(taskKey.getQueryId().toString());
        return resp;
    }
    
}
