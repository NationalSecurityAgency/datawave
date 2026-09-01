package datawave.core.query.logic;

import java.util.Collection;

import datawave.security.authorization.DatawavePrincipal;

/**
 * Context information provided to {@link ResponseRewriter} implementations. This provides access to request-level information needed for decisions such as
 * recipient determination in user-specific response filtering.
 *
 * <p>
 * The implementation of this interface (e.g., {@code AnnotationManagerRequestContext}) provides the concrete data from the active request.
 * </p>
 */
public interface ResponseRewriterContext {
    /**
     * @return the principal of the user making the request, or null if not a DataWave principal
     */
    DatawavePrincipal getDatawavePrincipal();

    /**
     * @return the distinguished name of the user making the request
     */
    String getUserDn();

    /**
     * @return the collection of proxy servers in the request chain
     */
    Collection<String> getProxyServers();
}
