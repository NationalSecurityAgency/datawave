package datawave.microservice.query;

import com.codahale.metrics.annotation.Timed;
import datawave.microservice.query.web.annotation.EnrichQueryMetrics;
import datawave.webservice.result.GenericResponse;
import org.springframework.http.MediaType;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping(path = "/v1", produces = MediaType.APPLICATION_JSON_VALUE)
public class QueryController {
    
    @Timed(name = "dw.query.defineQuery", absolute = true)
    @EnrichQueryMetrics(methodType = EnrichQueryMetrics.MethodType.CREATE)
    @RequestMapping(path = "{logicName}/define", method = {RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public GenericResponse<String> define(@PathVariable(name = "logicName") String logicName, @RequestParam MultiValueMap<String,String> parameters) {
        // validate query
        
        // persist the query w/ query id in the query cache
        
        // query tracing?
        
        // update query metrics (i.e. DEFINED)
        
        GenericResponse<String> resp = new GenericResponse<>();
        resp.setResult(UUID.randomUUID().toString());
        return resp;
    }
}
