package datawave.interceptor;

import java.io.IOException;

import jakarta.annotation.Priority;
import jakarta.interceptor.AroundInvoke;
import jakarta.interceptor.InvocationContext;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;

import datawave.Constants;
import datawave.webservice.common.exception.NoResultsException;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.BaseResponse;

/**
 *
 * Interceptor to be used on methods that return a BaseResponse object. This interceptor will add the timing information and exception information to the
 * response. Methods that use this interceptor should not throw any exceptions. Exceptions should be captured in the response object.
 *
 */
@Provider
@Priority(Priorities.HEADER_DECORATOR)
public class ResponseInterceptor implements ContainerResponseFilter {

    private static String ORIGIN = null;

    @AroundInvoke
    public Object invoke(InvocationContext ctx) throws Exception {
        long start = System.currentTimeMillis();
        Object r = ctx.proceed();
        boolean isResponseObject = r instanceof Response;
        boolean isBaseResponseObject = r instanceof BaseResponse;

        // If response type is not BaseResponse or subclass, then move on
        if (!isResponseObject && !isBaseResponseObject)
            return r;

        // Invoke the method
        if (isResponseObject) {
            Response result = (Response) r;
            long end = System.currentTimeMillis();
            result.getHeaders().add(Constants.OPERATION_TIME, (end - start));
            if (result.getEntity() instanceof BaseResponse)
                ((BaseResponse) result.getEntity()).setOperationTimeMS((end - start));
            return result;
        } else {
            BaseResponse result;
            try {
                result = (BaseResponse) r;
            } catch (NoResultsException e) {
                e.setStartTime(start);
                e.setEndTime(System.currentTimeMillis());
                throw e;
            }
            long end = System.currentTimeMillis();
            result.setOperationTimeMS((end - start));
            return result;
        }
    }

    @Override
    public void filter(ContainerRequestContext request, ContainerResponseContext response) throws IOException {
        if (null == ORIGIN) {
            ORIGIN = System.getProperty("cluster.name") + "/" + System.getProperty("jboss.host.name");
        }

        if (response.getEntity() instanceof BaseResponse) {
            BaseResponse br = (BaseResponse) response.getEntity();
            response.getHeaders().add(Constants.OPERATION_TIME, br.getOperationTimeMS());
        }
        if (response.getEntity() instanceof BaseQueryResponse) {
            BaseQueryResponse bqr = (BaseQueryResponse) response.getEntity();
            response.getHeaders().add(Constants.PAGE_NUMBER, bqr.getPageNumber());
            response.getHeaders().add(Constants.IS_LAST_PAGE, !bqr.getHasResults());
            response.getHeaders().add(Constants.PARTIAL_RESULTS, bqr.isPartialResults());
        }
        try {
            response.getHeaders().add(Constants.RESPONSE_ORIGIN, ORIGIN);
        } catch (UnsupportedOperationException e) {
            // this only happens when there's an error earlier in the chain
        }
    }
}
