package datawave.microservice.query;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(path = "/v1", produces = MediaType.APPLICATION_JSON_VALUE)
public class QueryController {
    
    // /**
    // * @param queryLogicName
    // * @param queryParameters
    // * @return
    // */
    // @POST
    // @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
    // "application/x-protostuff"})
    // @Path("/{logicName}/define")
    // @GZIP
    // @GenerateQuerySessionId(cookieBasePath = "/DataWave/Query/")
    // @EnrichQueryMetrics(methodType = MethodType.CREATE)
    // @Interceptors({RequiredInterceptor.class, ResponseInterceptor.class})
    // @Timed(name = "dw.query.defineQuery", absolute = true)
    // public GenericResponse<String> defineQuery(@Required("logicName") @PathParam("logicName") String queryLogicName,
    // MultivaluedMap<String,String> queryParameters, @Context HttpHeaders httpHeaders)
    @RequestMapping(path = "{logicName}/define", method = {RequestMethod.POST})
    public String define() {
        return null;
    }
    
}
