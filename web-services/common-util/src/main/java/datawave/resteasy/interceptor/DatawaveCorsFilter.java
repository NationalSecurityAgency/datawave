package datawave.resteasy.interceptor;

import org.jboss.resteasy.plugins.interceptors.CorsFilter;

import javax.ws.rs.container.PreMatching;
import javax.ws.rs.ext.Provider;
import java.util.concurrent.TimeUnit;

/**
 * This class adds the appropriate headers to allow CORS. There are no configurations, all methods are allowed. OPTIONS methods are not mapped JAX-RS methods
 * and therefore are not intercepted by ServerInterceptors. In order to add headers to the OPTIONS methods, the DefaultOptionsMethodException must mapped and
 * caught.
 *
 * This class made the following request work from another domain:
 *
 * <pre>
 * {@code
 *
 * var query = { pagesize: 1, auths: 'A,B,C,D,E,F,G', queryName: 'Datawave Query Test', expiration: '20120912 235959', logicName: 'EventQuery', query:
 * 'criteria', begin: '20120702', end: '20120702' }
 *
 * var urlEncodeObject = function(options) { var str = "" $.each(options, function(key, value) { str += key + '=' + encodeURIComponent(value) + '&'; }) return
 * str.substr(0, str.length - 1).replace(/%20/g, '+') }
 *
 * var client = new XMLHttpRequest() client.withCredentials = true client.onreadystatechange = function() {} client.open('POST', host +
 * '/DataWave/Query/create.json') client.setRequestHeader("Content-Type", "application/x-www-form-urlencoded")
 *
 * var params = urlEncodeObject(query) console.log(params) client.setRequestHeader("Content-length", params.length) client.send(params)
 * }
 * </pre>
 */
@Provider
@PreMatching
public class DatawaveCorsFilter extends CorsFilter {
    public static final String ALLOWED_METHODS = "HEAD, DELETE, GET, POST, PUT, OPTIONS";
    public static final String ALLOWED_HEADERS = "X-SSL-ClientCert-Subject, X-ProxiedEntitiesChain, X-ProxiedIssuersChain, Accept, Accept-Encoding";
    public static final int MAX_AGE = (int) TimeUnit.DAYS.toSeconds(10);
    public static final boolean ALLOW_CREDENTIALS = true;

    public DatawaveCorsFilter() {
        setAllowedMethods(ALLOWED_METHODS);
        setAllowedHeaders(ALLOWED_HEADERS);
        setAllowCredentials(ALLOW_CREDENTIALS);
        setCorsMaxAge(MAX_AGE);
        allowedOrigins.add("*");
    }
}
