package datawave.accumulo.util.security;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.accumulo.core.security.Authorizations;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;

import datawave.security.authorization.AuthorizationException;
import datawave.security.authorization.DatawaveUser;

/**
 * Function definitions for common DataWave/Accumulo security concerns, such as translation of {@link DatawaveUser} auth tokens to Accumulo
 * {@link Authorizations}, etc. Default implementation provided.
 *
 * @see datawave.accumulo.util.security.UserAuthFunctions.Default
 */
public interface UserAuthFunctions {
    
    /**
     * System property for overriding {@link Default} instance at runtime, if desired.
     * 
     * @see #getInstance()
     */
    String DEFAULT_CLASS_OVERRIDE_PROPERTY = "datawave.user.auth.functions.class";
    
    char REQUESTED_AUTHS_DELIMITER = ',';
    
    /**
     * If {@code requestedAuths} contains elements that don't appear in {@link DatawaveUser#getAuths()}, implementors will throw an exception indicating that
     * authorizations have been requested which the user has not been granted.
     *
     * @param requestedAuths
     *            The set of requested Accumulo authorizations (comma-delimited)
     * @param user
     *            The DataWave user for whom authorizations are being requested
     *            
     * @throws AuthorizationException
     *             when the requested auths are not valid for the user
     */
    void validateRequestedAuthorizations(String requestedAuths, DatawaveUser user) throws AuthorizationException;
    
    /**
     * Typically, implementations will simply compute the intersection of the two sets, {@code requestedAuths} and {@link DatawaveUser#getAuths()}, and then
     * return the resulting set in the form of an {@link Authorizations} instance.
     * <p>
     * For example, computing the intersection of the sets can allow a DataWave user to request a "downgraded" subset of their granted auths for their pending
     * query while also preventing privilege escalation.
     * <p>
     * If {@code requestedAuths} contains elements that don't appear in {@link DatawaveUser#getAuths()}, implementors should throw a runtime exception
     * indicating that authorizations have been requested which the user has not been granted.
     * <p>
     * Additionally, implementors should return a default {@link Authorizations} instance (i.e., {@code new Authorizations()}) rather than {@code null} to
     * denote that the request is invalid based on the given inputs.
     *
     * @param requestedAuths
     *            The set of requested Accumulo authorizations (comma-delimited)
     * @param user
     *            The DataWave user for whom authorizations are being requested
     *            
     * @return {@link Authorizations} instance appropriate for the given inputs
     */
    Authorizations getRequestedAuthorizations(String requestedAuths, DatawaveUser user);
    
    /**
     * Typically, implementations will simply compute the intersection of the two sets, {@code requestedAuths} and {@link DatawaveUser#getAuths()}, and then
     * return the resulting set in the form of an {@link Authorizations} instance.
     * <p>
     * For example, computing the intersection of the sets can allow a DataWave user to request a "downgraded" subset of their granted auths for their pending
     * query while also preventing privilege escalation.
     * <p>
     * If {@code throwOnMissingAuths} is true and the {@code requestedAuths} contains elements that don't appear in {@link DatawaveUser#getAuths()},
     * implementors will throw a runtime exception indicating that authorizations have been requested which the user has not been granted.
     * <p>
     * Additionally, implementors should return a default {@link Authorizations} instance (i.e., {@code new Authorizations()}) rather than {@code null} to
     * denote that the request is invalid based on the given inputs.
     *
     * @param requestedAuths
     *            The set of requested Accumulo authorizations (comma-delimited)
     * @param user
     *            The DataWave user for whom authorizations are being requested
     * @param throwOnMissingAuths
     *            If true then an exception is thrown if the user is missing any of the requested auths
     *            
     * @return {@link Authorizations} instance appropriate for the given inputs
     */
    Authorizations getRequestedAuthorizations(String requestedAuths, DatawaveUser user, boolean throwOnMissingAuths);
    
    /**
     * This method provides a merged view of the auths from the specified proxy chain and from those in {@code primaryUserAuths}, e.g., as precomputed by
     * {@link #getRequestedAuthorizations(String, DatawaveUser)} perhaps.
     *
     * <p>
     * The resulting set may be applied by DataWave at scan time to ensure that any data returned from Accumulo is suitable for any/all users in the given chain
     *
     * @param primaryUserAuths
     *            Authorizations associated with the "primary" user, i.e., the initiating user to whom scan results will ultimately be delivered
     * @param proxyChain
     *            Collection of proxies denoting the complete user chain for a given request
     * @param proxiedUserTest
     *            A predicate that, given a user from {@code proxyChain}, indicates whether or not that user is the primary user. The predicate should return
     *            {@code true} if the user is NOT the primary user.
     *            
     * @return A set of {@link Authorizations}, one per user entity represented by the user chain including {@code primaryUserAuths}
     */
    LinkedHashSet<Authorizations> mergeAuthorizations(Authorizations primaryUserAuths, Collection<? extends DatawaveUser> proxyChain,
                    Predicate<? super DatawaveUser> proxiedUserTest);
    
    /**
     * Default implementation of {@link UserAuthFunctions}
     */
    class Default implements UserAuthFunctions {
        
        /**
         *
         * @throws AuthorizationException
         *             if {@code requestedAuths} contains elements that do not exist in {@link DatawaveUser#getAuths()}
         */
        @Override
        public void validateRequestedAuthorizations(String requestedAuths, DatawaveUser user) throws AuthorizationException {
            try {
                getRequestedAuthorizations(requestedAuths, user, true);
            } catch (IllegalArgumentException e) {
                throw new AuthorizationException(e);
            }
        }
        
        /**
         *
         * @return IFF every element in {@code requestedAuths} also exists in {@link DatawaveUser#getAuths()} then {@code requestedAuths} is translated to
         *         {@link Authorizations} and returned. If either of {@code requestedAuths} or {@link DatawaveUser#getAuths()} is empty/null, then
         *         {@link Authorizations#EMPTY} is returned. If any requested auths are missing in {@link DatawaveUser#getAuths()}, then an exception is thrown
         *         
         * @throws IllegalArgumentException
         *             if {@code requestedAuths} contains elements that do not exist in {@link DatawaveUser#getAuths()}
         */
        @Override
        public Authorizations getRequestedAuthorizations(String requestedAuths, DatawaveUser user) {
            return getRequestedAuthorizations(requestedAuths, user, true);
        }
        
        /**
         *
         * @return IFF every element in {@code requestedAuths} also exists in {@link DatawaveUser#getAuths()} then {@code requestedAuths} is translated to
         *         {@link Authorizations} and returned. If either of {@code requestedAuths} or {@link DatawaveUser#getAuths()} is empty/null, then
         *         {@link Authorizations#EMPTY} is returned. If any requested auths are missing in {@link DatawaveUser#getAuths()}, then an exception is thrown
         *         
         * @throws IllegalArgumentException
         *             if {@code requestedAuths} contains elements that do not exist in {@link DatawaveUser#getAuths()}
         */
        @Override
        public Authorizations getRequestedAuthorizations(String requestedAuths, DatawaveUser user, boolean throwOnMissingAuths) {
            if (null == user) {
                return Authorizations.EMPTY;
            }
            return UserAuthFunctions.getRequestedAuthorizations(requestedAuths, user::getAuths, throwOnMissingAuths);
        }
        
        /**
         * If the specified {@code primaryUserAuths} is {@code null}, then a set of size = 1 will will be returned, and its one element will be a default
         * Authorizations instance (i.e., {@code new Authorizations()}
         */
        @Override
        public LinkedHashSet<Authorizations> mergeAuthorizations(Authorizations primaryUserAuths, Collection<? extends DatawaveUser> proxyChain,
                        Predicate<? super DatawaveUser> proxiedUserTest) {
            LinkedHashSet<Authorizations> mergedAuths = new LinkedHashSet<>();
            
            if (null == primaryUserAuths) {
                mergedAuths.add(Authorizations.EMPTY);
            } else {
                mergedAuths.add(primaryUserAuths);
                
                if (null != proxyChain) {
                    // Now simply add the auths from each non-primary user to the merged auths set
                    // @formatter:off
                    proxyChain.stream()
                            .filter(proxiedUserTest)
                            .map(DatawaveUser::getAuths)
                            .map(UserAuthFunctions::toAuthorizations)
                            .forEach(mergedAuths::add);
                    // @formatter:on
                }
            }
            return mergedAuths;
        }
    }
    
    /**
     * @return If {@code throwOnMissingAuths} is {@code false}, returns the intersection of {@code requestedAuths} and {@code authSupplier.get()} as an
     *         {@link Authorizations} instance. If {@code throwOnMissingAuths} is {@code true}, the same intersection is computed, but
     *         {@link IllegalArgumentException} is thrown if any auths were requested but not supplied.
     */
    static Authorizations getRequestedAuthorizations(String requestedAuths, Supplier<Collection<String>> authSupplier, boolean throwOnMissingAuths) {
        HashSet<String> requested = null;
        if (!Strings.isNullOrEmpty(requestedAuths)) {
            requested = new HashSet<>(splitAuths(requestedAuths));
        }
        
        Authorizations authorizations = null;
        
        if (null != authSupplier) {
            HashSet<String> missingAuths = (requested == null) ? new HashSet<>() : new HashSet<>(requested);
            HashSet<String> userAuths = new HashSet<>(authSupplier.get());
            if (!userAuths.isEmpty()) {
                if (null != requested) {
                    missingAuths.removeAll(userAuths);
                    userAuths.retainAll(requested);
                }
                authorizations = new Authorizations(userAuths.toArray(new String[userAuths.size()]));
            }
            
            if (!missingAuths.isEmpty() && throwOnMissingAuths) {
                throw new IllegalArgumentException("User requested authorizations that they don't have. Missing: " + missingAuths + ", Requested: " + requested
                                + ", User: " + userAuths);
            }
        }
        
        if (null == authorizations) {
            authorizations = Authorizations.EMPTY;
        }
        return authorizations;
    }
    
    /**
     * Converts comma-delimited {@code requestedAuths} to a list
     * 
     * @param requestedAuths
     *            comma-delimited list of authorizations
     * @return List containing the auth tokens from {@code requestedAuths}
     */
    static List<String> splitAuths(String requestedAuths) {
        return Arrays.asList(Iterables.toArray(Splitter.on(REQUESTED_AUTHS_DELIMITER).omitEmptyStrings().trimResults().split(requestedAuths), String.class));
    }
    
    /**
     * Converts auths string collection to an {@link Authorizations} instance
     * 
     * @param auths
     *            Auths to convert
     * @return {@link Authorizations} equivalent of the specified string collection
     */
    static Authorizations toAuthorizations(Collection<String> auths) {
        return new Authorizations(auths.stream().map(String::trim).map(s -> s.getBytes(UTF_8)).collect(Collectors.toList()));
    }
    
    /**
     * Gets a {@link Default} instance, which may be overridden at runtime via system property {@link #DEFAULT_CLASS_OVERRIDE_PROPERTY}, if desired. Impl
     * overrides must be thread-safe
     *
     * @return UserAuthFunctions instance
     */
    static UserAuthFunctions getInstance() {
        return Holder.INSTANCE;
    }
    
    /**
     * On-demand holder for UserAuthFunctions singleton
     */
    class Holder {
        private static final UserAuthFunctions INSTANCE = createUserAuthFunctions();
        
        private Holder() {}
        
        private static UserAuthFunctions createUserAuthFunctions() {
            final String classOverride = System.getProperty(DEFAULT_CLASS_OVERRIDE_PROPERTY);
            if (null == classOverride) {
                return new UserAuthFunctions.Default();
            } else {
                try {
                    return (UserAuthFunctions) Class.forName(classOverride).getDeclaredConstructor().newInstance();
                } catch (Throwable t) {
                    throw new RuntimeException(String.format("Failed to create instance of '%s'", classOverride), t);
                }
            }
        }
    }
}
