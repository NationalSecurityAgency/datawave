package datawave.resteasy.interceptor;

import datawave.Constants;
import datawave.annotation.ClearQuerySessionId;
import org.jboss.resteasy.util.HttpHeaderNames;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.NewCookie;
import javax.ws.rs.ext.Provider;
import java.io.IOException;

/**
 * JAX-RS filter to clear the {@link Constants#QUERY_COOKIE_NAME} cookie value.
 */
@Provider
@ClearQuerySessionId
public class ClearQuerySessionIDFilter implements ContainerResponseFilter {
    @Override
    public void filter(ContainerRequestContext request, ContainerResponseContext response) throws IOException {
        response.getHeaders().add(HttpHeaderNames.SET_COOKIE, new NewCookie(Constants.QUERY_COOKIE_NAME, ""));
    }
}
