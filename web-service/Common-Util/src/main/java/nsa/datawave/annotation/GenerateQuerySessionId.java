package nsa.datawave.annotation;

import javax.ws.rs.NameBinding;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A {@link NameBinding} to allow annotation of methods that are designated to create a new query and therefore set a cookie with an id for the query. Load
 * balancers can use this query to stick other requests for the query (e.g., next, close) to the web server that created the query, and therefore the one that
 * has the resources needed to evaluate the query.
 *
 * @see nsa.datawave.annotation.ClearQuerySessionId
 * @see nsa.datawave.resteasy.interceptor.CreateQuerySessionIDFilter
 */
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@NameBinding
public @interface GenerateQuerySessionId {
    /**
     * @return The base path for the generated cookie. The base path will be combined with the query id in order to form the cookie domain.
     */
    String cookieBasePath();
}
