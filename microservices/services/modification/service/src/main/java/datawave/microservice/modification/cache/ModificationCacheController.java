package datawave.microservice.modification.cache;

import static datawave.microservice.http.converter.protostuff.ProtostuffHttpMessageConverter.PROTOSTUFF_VALUE;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.codahale.metrics.annotation.Timed;

import datawave.modification.cache.ModificationCache;
import datawave.webservice.result.VoidResponse;
import datawave.webservice.results.modification.MutableFieldListResponse;

@RestController
@RequestMapping(path = "/v1", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE, MediaType.TEXT_XML_VALUE, PROTOSTUFF_VALUE,
        "text/x-yaml", "application/x-yaml", "text/yaml", "application/x-protobuf"})
public class ModificationCacheController {
    
    private final ModificationCache cache;
    
    public ModificationCacheController(ModificationCache cache) {
        this.cache = cache;
    }
    
    /**
     * @return datawave.webservice.result.VoidResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @ResponseHeader query-session-id this header and value will be in the Set-Cookie header, subsequent calls for this session will need to supply the
     *                 query-session-id header in the request in a Cookie header or as a query parameter
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     * @HTTP 200 success
     * @HTTP 202 if asynch is true - see Location response header for the job URI location
     * @HTTP 400 invalid or missing parameter
     * @HTTP 500 internal server error
     */
    @GetMapping("/reloadCache")
    @Timed(name = "dw.modification.reload.cache", absolute = true)
    public VoidResponse reloadMutableFieldCache() {
        this.cache.reloadMutableFieldCache();
        return new VoidResponse();
    }
    
    @GetMapping("/getMutableFieldList")
    @Timed(name = "dw.modification.mutable.field.list", absolute = true)
    public List<MutableFieldListResponse> getMutableFieldList() {
        List<MutableFieldListResponse> lists = new ArrayList<>();
        for (Map.Entry<String,Set<String>> entry : this.cache.getCachedMutableFieldList().entrySet()) {
            MutableFieldListResponse r = new MutableFieldListResponse();
            r.setDatatype(entry.getKey());
            r.setMutableFields(entry.getValue());
            lists.add(r);
        }
        return lists;
    }
}
