package datawave.microservice.query;

import com.codahale.metrics.annotation.Timed;
import datawave.microservice.query.web.annotation.GenerateQuerySessionId;
import datawave.webservice.result.BaseQueryResponse;
import org.springframework.http.MediaType;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.security.RolesAllowed;

@RestController
@RequestMapping(path = "/v1", produces = MediaType.APPLICATION_JSON_VALUE)
public class QueryController {
    
    // /**
    // * @param queryLogicName
    // * @param queryParameters
    // * @return
    // */
    // X @POST
    // X @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
    // "application/x-protostuff"})
    // X @Path("/{logicName}/define")
    // X @GZIP
    // X @GenerateQuerySessionId(cookieBasePath = "/DataWave/Query/")
    // @EnrichQueryMetrics(methodType = MethodType.CREATE)
    // X @Interceptors({RequiredInterceptor.class, ResponseInterceptor.class})
    // X @Timed(name = "dw.query.defineQuery", absolute = true)
    // X public GenericResponse<String> defineQuery(@Required("logicName") @PathParam("logicName") String queryLogicName,
    // X MultivaluedMap<String,String> queryParameters, @Context HttpHeaders httpHeaders)
    // NOTE: The goal is to not use this, but it's here if we need it.
    @RolesAllowed({"AuthorizedUser", "AuthorizedServer", "InternalUser", "Administrator"})
    @GenerateQuerySessionId(cookieBasePath = "/DataWave/Query/")
    @Timed(name = "dw.query.defineQuery", absolute = true) // TODO: Figure out where this is used
    @RequestMapping(path = "{logicName}/define", method = {RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public BaseQueryResponse define(@PathVariable(name = "logicName") String logicName, @RequestParam MultiValueMap<String,String> parameters) {
        // QuerySessionIdContext.setQueryId("some-query-id");
        // GenericResponse<String> resp = new GenericResponse<>();
        // resp.setResult("something something");
        // return resp;
        BaseQueryResponse bqr = new BaseQueryResponse() {
            
        };
        
        return bqr;
    }
}
