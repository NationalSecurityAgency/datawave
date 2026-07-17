package datawave.microservice.annotation.util.lookup.common;

import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationList;
import datawave.annotation.util.v1.AnnotationJsonUtils;
import datawave.microservice.annotation.util.exceptions.AuthenticationException;
import datawave.microservice.annotation.util.exceptions.AuthorizationException;
import datawave.microservice.annotation.util.exceptions.BadRequestException;
import datawave.microservice.annotation.util.exceptions.ConflictException;
import datawave.microservice.annotation.util.exceptions.InternalServerException;
import datawave.microservice.annotation.util.exceptions.MethodNotAllowedException;
import datawave.microservice.annotation.util.exceptions.PayloadTooLargeException;
import datawave.microservice.annotation.util.exceptions.PreconditionFailedException;
import datawave.microservice.annotation.util.exceptions.RangeNotSatisfiableException;
import datawave.microservice.annotation.util.exceptions.ResourceNotFoundException;
import datawave.microservice.annotation.util.exceptions.ServiceUnavailableException;
import datawave.microservice.annotation.util.lookup.config.LookupProperties;
import datawave.webservice.result.BaseResponse;
import lombok.extern.slf4j.Slf4j;

/**
 * Encapsulates Datawave lookup query requests
 */
@Slf4j
@Component
public class LookupRequest {
    private final CloseableHttpClient httpClient;
    private final LookupProperties lookupProperties;
    private final BasicCookieStore cookieStore;
    private static final String PROTOCOL = "https";
    private static final String PARAM_KEY = "params";
    private static final String METADATA_URI = "DataWave/Query/lookupUUID/";
    private static final String CONTENT_URI = "DataWave/Query/lookupContentUUID/";
    private static final String MARKINGS_URI = "DataWave/Annotations/v1/";

    @Autowired
    public LookupRequest(CloseableHttpClient httpClient, LookupProperties lookupProperties) {
        this.httpClient = httpClient;
        this.lookupProperties = lookupProperties;
        cookieStore = new BasicCookieStore();
    }

    public ParsedResponse lookupId(Lookup lookup, String lookupPath, Map<String,String> headers, String queryParams, String systemFrom) {
        // Set request parameters
        HttpGet lookupQuery = buildLookupQueryRequest(lookup, lookupPath, headers, queryParams, systemFrom);
        HttpContext context = getHttpContext();

        try (CloseableHttpResponse response = httpClient.execute(lookupQuery, context)) {
            int responseCode = response.getStatusLine().getStatusCode();

            ParsedResponse parsedResponse = new ParsedResponse();
            parsedResponse.setResponse(processResponse(response));
            parsedResponse.setCode(responseCode);
            return parsedResponse;
        } catch (IOException e) {
            log.error("Failed to execute datawave query: {}", lookupQuery, e);
            throw new InternalServerException(e);
        } finally {
            lookupQuery.reset();
        }
    }

    private String processResponse(CloseableHttpResponse response) throws IOException {
        String rawResponse = null;
        HttpEntity entity = response.getEntity();
        if (entity != null) {
            rawResponse = EntityUtils.toString(entity, StandardCharsets.UTF_8);
        }
        validateResponse(response.getStatusLine().getStatusCode(), rawResponse);
        return rawResponse;
    }

    private void validateResponse(int responseCode, String rawResponse) {
        if (responseCode < 200 || responseCode >= 300) {
            switch (responseCode) {
                case 400:
                    throw new BadRequestException(rawResponse);
                case 401:
                    throw new AuthenticationException(rawResponse);
                case 403:
                    throw new AuthorizationException(rawResponse);
                case 404:
                    throw new ResourceNotFoundException(rawResponse);
                case 405:
                    throw new MethodNotAllowedException(rawResponse);
                case 409:
                    throw new ConflictException(rawResponse);
                case 412:
                    throw new PreconditionFailedException(rawResponse);
                case 413:
                    throw new PayloadTooLargeException(rawResponse);
                case 416:
                    throw new RangeNotSatisfiableException(rawResponse);
                case 503:
                    throw new ServiceUnavailableException(rawResponse);
                default:
                    // Catch 500 as well as any other codes not explicitly listed
                    throw new InternalServerException(rawResponse);
            }
        } else if (rawResponse == null || rawResponse.isBlank()) {
            throw new ResourceNotFoundException("No Content");
        }
    }

    private HttpContext getHttpContext() {
        HttpContext context = new BasicHttpContext();
        context.setAttribute(HttpClientContext.COOKIE_STORE, cookieStore);
        context.setAttribute("X-Start-Time", String.valueOf(System.currentTimeMillis()));
        return context;
    }

    /**
     * Create lookup query URL from config properties, ID, and queryParams
     *
     * @param lookup
     *            - the string to determine the DataWave endpoint being invoked
     * @param lookupPath
     *            - the lookupPath, either type + id, id, or uuid pairs (depending on endpoint invoked)
     * @param headers
     *            - headers for the lookup query
     * @param queryParams
     *            - parameters to be passed to the lookup query
     * @param systemFrom
     *            - the system making the request
     * @return - an HttpGet object
     */
    protected HttpGet buildLookupQueryRequest(Lookup lookup, String lookupPath, Map<String,String> headers, String queryParams, String systemFrom) {
        String uri;
        try {
            switch (lookup) {
                case CONTENT:
                    uri = CONTENT_URI;
                    break;
                case MARKINGS:
                    uri = MARKINGS_URI;
                    break;
                case METADATA:
                    uri = METADATA_URI;
                    break;
                default:
                    uri = null;
                    break;
            }
            // @formatter:off
            URIBuilder builder = new URIBuilder()
                    .setScheme(PROTOCOL)
                    .setHost(lookupProperties.getDatawaveQueryHost())
                    .setPath(uri + lookupPath)
                    .setParameter(PARAM_KEY, queryParams);
            // @formatter:on
            URI lookupRequest = new URI(builder.build().toString() + "&systemFrom=" + systemFrom);
            HttpGet get = new HttpGet(lookupRequest);
            for (Map.Entry<String,String> header : headers.entrySet()) {
                get.setHeader(header.getKey(), header.getValue());
            }
            return get;
        } catch (URISyntaxException e) {
            throw new InternalServerException("Failed to parse query from request.", e);
        }
    }

    public BaseResponse parseQueryResponse(String rawResponse) {
        try {
            JAXBContext jc = JAXBContext.newInstance(lookupProperties.getDatawaveResponseClasses(), null);
            Unmarshaller unmarshaller = jc.createUnmarshaller();
            return (BaseResponse) unmarshaller.unmarshal(new StringReader(rawResponse));
        } catch (JAXBException e) {
            throw new InternalServerException("Failed to parse DataWave Query response.", e);
        }
    }

    public List<Annotation> parseAnnotationResponse(String rawResponse) {
        if (rawResponse == null || rawResponse.isBlank()) {
            throw new ResourceNotFoundException("Datawave Annotation response was Blank");
        }
        if (rawResponse.startsWith("[") && rawResponse.endsWith("]")) {
            rawResponse = "{\n  \"annotations\": " + rawResponse + "\n}";
        }

        JsonFormat.Parser parser = AnnotationJsonUtils.getParser();
        AnnotationList.Builder builder = AnnotationList.newBuilder();

        try {
            if (lookupProperties.isAnnotationUnknownFieldsIgnored()) {
                parser.ignoringUnknownFields().merge(rawResponse, builder);
            } else {
                parser.merge(rawResponse, builder);
            }
        } catch (InvalidProtocolBufferException e) {
            throw new InternalServerException("Failed to parse DataWave Annotation response.", e);
        }
        AnnotationList aas = builder.build();
        return new ArrayList<>(aas.getAnnotationsList());
    }
}
