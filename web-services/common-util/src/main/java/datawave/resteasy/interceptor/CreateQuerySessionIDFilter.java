package datawave.resteasy.interceptor;

import java.io.IOException;
import java.util.UUID;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.NewCookie;
import javax.ws.rs.ext.Provider;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.jboss.resteasy.core.ResourceMethodInvoker;
import org.jboss.resteasy.util.FindAnnotation;
import org.jboss.resteasy.util.HttpHeaderNames;

import datawave.Constants;
import datawave.annotation.GenerateQuerySessionId;

/**
 * JAX-RS filter to create a {@link Constants#QUERY_COOKIE_NAME} cookie.
 */
@Provider
@GenerateQuerySessionId(cookieBasePath = "notNeededHere")
public class CreateQuerySessionIDFilter implements ContainerResponseFilter {
    private final Logger log = Logger.getLogger(CreateQuerySessionIDFilter.class);
    public static final ThreadLocal<String> QUERY_ID = new ThreadLocal<>();

    @Override
    public void filter(ContainerRequestContext request, ContainerResponseContext response) throws IOException {
        ResourceMethodInvoker method = (ResourceMethodInvoker) request.getProperty(ResourceMethodInvoker.class.getName());

        GenerateQuerySessionId annotation = FindAnnotation.findAnnotation(method.getMethodAnnotations(), GenerateQuerySessionId.class);

        String path = annotation.cookieBasePath();
        String id = "";
        String cookieValue = generateCookieValue();
        boolean setCookie = true;
        switch (response.getStatusInfo().getFamily()) {
            case SERVER_ERROR:
            case CLIENT_ERROR:
                // If we're sending an error response, then there's no need to set a cookie since
                // there's no query "session" to stick to this server.
                setCookie = false;
                QUERY_ID.set(null);
                break;

            default:
                if (StringUtils.isEmpty(QUERY_ID.get())) {
                    log.error(method.getResourceClass() + "." + method.getMethod().getName() + " did not set QUERY_ID threadlocal.");
                } else {
                    id = QUERY_ID.get();
                    QUERY_ID.set(null);
                }
                break;
        }

        if (setCookie) {
            response.getHeaders().add(HttpHeaderNames.SET_COOKIE,
                            new NewCookie(Constants.QUERY_COOKIE_NAME, cookieValue, path + id, null, null, NewCookie.DEFAULT_MAX_AGE, false));
        }
    }

    public static String generateCookieValue() {
        return Integer.toString(UUID.randomUUID().hashCode() & Integer.MAX_VALUE);
    }
}
