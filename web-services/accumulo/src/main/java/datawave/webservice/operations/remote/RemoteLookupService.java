package datawave.webservice.operations.remote;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;

import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.message.BasicNameValuePair;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.databind.ObjectReader;

import datawave.webservice.response.LookupResponse;

public class RemoteLookupService extends RemoteAccumuloService {

    private static final String LOOKUP_SUFFIX = "lookup/%s/%s";

    private ObjectReader lookupReader;

    @Override
    @PostConstruct
    public void init() {
        super.init();
        lookupReader = objectMapper.readerFor(LookupResponse.class);
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
                httpPost.setHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON);
            },
            entity -> lookupReader.readValue(entity.getContent()),
            () -> suffix + " [" + params + "]");
        // @formatter:on
    }
}
