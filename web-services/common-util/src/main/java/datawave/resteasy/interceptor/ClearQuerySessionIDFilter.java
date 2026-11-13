package datawave.resteasy.interceptor;

import java.io.IOException;

import org.jboss.resteasy.util.HttpHeaderNames;

import datawave.Constants;
import datawave.annotation.ClearQuerySessionId;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.core.NewCookie;
import jakarta.ws.rs.ext.Provider;

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
