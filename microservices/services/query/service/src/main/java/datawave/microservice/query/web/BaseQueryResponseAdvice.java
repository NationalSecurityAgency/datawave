package datawave.microservice.query.web;

import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.core.MethodParameter;
import org.springframework.http.MediaType;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.lang.NonNull;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyAdvice;

import datawave.Constants;
import datawave.webservice.result.BaseQueryResponse;

/**
 * A {@link ControllerAdvice} that implements {@link ResponseBodyAdvice} in order to allow access to {@link BaseQueryResponse} objects before they are written
 * out to the response body. This is primarily used to write the page number, is last page, and partial results headers for the response.
 */
@ControllerAdvice
@ConditionalOnClass(BaseQueryResponse.class)
public class BaseQueryResponseAdvice implements ResponseBodyAdvice<BaseQueryResponse> {
    
    @Override
    public boolean supports(@NonNull MethodParameter returnType, @NonNull Class converterType) {
        return BaseQueryResponse.class.isAssignableFrom(returnType.getParameterType());
    }
    
    @Override
    public BaseQueryResponse beforeBodyWrite(BaseQueryResponse baseQueryResponse, @NonNull MethodParameter returnType, @NonNull MediaType selectedContentType,
                    @NonNull Class selectedConverterType, @NonNull ServerHttpRequest request, ServerHttpResponse response) {
        response.getHeaders().add(Constants.PAGE_NUMBER, String.valueOf(baseQueryResponse.getPageNumber()));
        response.getHeaders().add(Constants.IS_LAST_PAGE, String.valueOf(!baseQueryResponse.getHasResults()));
        response.getHeaders().add(Constants.PARTIAL_RESULTS, String.valueOf(baseQueryResponse.isPartialResults()));
        return baseQueryResponse;
    }
}
