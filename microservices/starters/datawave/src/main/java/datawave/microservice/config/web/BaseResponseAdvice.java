package datawave.microservice.config.web;

import static datawave.microservice.config.web.Constants.OPERATION_TIME_MS_HEADER;
import static datawave.microservice.config.web.Constants.REQUEST_START_TIME_NS_ATTRIBUTE;

import java.util.concurrent.TimeUnit;

import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.core.MethodParameter;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.http.server.ServletServerHttpRequest;
import org.springframework.lang.NonNull;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyAdvice;

import datawave.webservice.result.BaseResponse;

/**
 * A {@link ControllerAdvice} that implements {@link ResponseBodyAdvice} in order to allow access to {@link BaseResponse} objects before they are written out to
 * the response body. This is primarily used to write the operation time into the {@link BaseResponse#setOperationTimeMS(long)} property.
 */
@ControllerAdvice
@ConditionalOnClass(BaseResponse.class)
public class BaseResponseAdvice implements ResponseBodyAdvice<BaseResponse> {
    @Override
    public boolean supports(@NonNull MethodParameter returnType, @NonNull Class<? extends HttpMessageConverter<?>> converterType) {
        return BaseResponse.class.isAssignableFrom(returnType.getParameterType());
    }
    
    @Override
    public BaseResponse beforeBodyWrite(BaseResponse baseResponse, @NonNull MethodParameter returnType, @NonNull MediaType selectedContentType,
                    @NonNull Class<? extends HttpMessageConverter<?>> selectedConverterType, @NonNull ServerHttpRequest serverHttpRequest,
                    @NonNull ServerHttpResponse serverHttpResponse) {
        if (serverHttpRequest instanceof ServletServerHttpRequest) {
            ServletServerHttpRequest sr = (ServletServerHttpRequest) serverHttpRequest;
            Long startTimeNS = (Long) sr.getServletRequest().getAttribute(REQUEST_START_TIME_NS_ATTRIBUTE);
            if (startTimeNS != null) {
                long operationTimeMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNS);
                baseResponse.setOperationTimeMS(operationTimeMillis);
                serverHttpResponse.getHeaders().set(OPERATION_TIME_MS_HEADER, Long.toString(operationTimeMillis));
            }
        }
        return baseResponse;
    }
}
