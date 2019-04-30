package datawave.webservice.operations.remote;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationIntrospector;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationModule;
import datawave.configuration.RefreshableScope;
import datawave.webservice.response.LookupResponse;
import datawave.webservice.response.objects.DefaultKey;
import datawave.webservice.response.objects.KeyBase;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.message.BasicNameValuePair;

import javax.annotation.PostConstruct;
import javax.annotation.Priority;
import javax.enterprise.inject.Alternative;
import javax.interceptor.Interceptor;
import javax.ws.rs.core.MultivaluedMap;
import java.util.ArrayList;
import java.util.List;

@RefreshableScope
@Alternative
@Priority(Interceptor.Priority.APPLICATION)
public class RemoteLookupService extends RemoteAccumuloService {
    
    private static final String LOOKUP_SUFFIX = "lookup/%s/%s";
    
    private ObjectMapper lookupMapper;
    private ObjectReader lookupReader;
    
    @Override
    @PostConstruct
    public void init() {
        super.init();
        lookupMapper = new ObjectMapper();
        lookupMapper.registerModule(new JaxbAnnotationModule());
        lookupMapper.setAnnotationIntrospector(new JaxbAnnotationIntrospector(TypeFactory.defaultInstance()));
        SimpleModule module = new SimpleModule("LookupResponse.DefaultKey", Version.unknownVersion());
        module.addAbstractTypeMapping(KeyBase.class, DefaultKey.class);
        lookupMapper.registerModule(module);
        lookupReader = lookupMapper.readerFor(LookupResponse.class);
    }
    
    @Timed(name = "dw.remoteAccumuloService.lookup", absolute = true)
    public LookupResponse lookup(String table, String row, MultivaluedMap<String,String> params) {
        
        final List<NameValuePair> nvpList = new ArrayList<>();
        params.forEach((k, valueList) -> valueList.forEach(v -> nvpList.add(new BasicNameValuePair(k, v))));
        final UrlEncodedFormEntity postBody = new UrlEncodedFormEntity(nvpList::iterator);
        
        String suffix = String.format(LOOKUP_SUFFIX, table, row);
        
        // @formatter:off
        return executePostMethodWithRuntimeException(
            suffix,
            uriBuilder -> {},
            httpPost -> {
                httpPost.setEntity(postBody);
                httpPost.setHeader(AUTH_HEADER_NAME, getBearer());
            },
            entity -> lookupReader.readValue(entity.getContent()),
            () -> suffix + " [" + params + "]");
        // @formatter:on
    }
}
