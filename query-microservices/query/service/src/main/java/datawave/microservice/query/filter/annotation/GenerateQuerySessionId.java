package datawave.microservice.query.filter.annotation;

import java.lang.annotation.*;

/**
 * An annotation which is used to identify methods that are designated to create a new query and therefore set a cookie with an id for the query. Load balancers
 * can use this query to stick other requests for the query (e.g., next, close) to the web server that created the query, and therefore the one that has the
 * resources needed to evaluate the query.
 *
 * @see datawave.annotation.ClearQuerySessionId
 * @see datawave.resteasy.interceptor.CreateQuerySessionIDFilter
 */
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface GenerateQuerySessionId {
    /**
     * @return The base path for the generated cookie. The base path will be combined with the query id in order to form the cookie domain.
     */
    String cookieBasePath();
}
