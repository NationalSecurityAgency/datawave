package datawave.microservice.authorization.util;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Splitter;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import datawave.accumulo.util.security.UserAuthFunctions;
import datawave.security.authorization.AuthorizationException;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.ProxiedUserDetails;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.util.AuthorizationsMinimizer;

/**
 * Several of these methods refer to different types of datawaveUserDetailss:
 *
 * overallUserDetails: This is the user details that represents all of the possible auths that the calling user is allowed to have. The requested auths must
 * always be a subset of these. This will be a combination of the local user details (the one for this webserver) and the user details for and remote user
 * operations that may be applicable. queryUserDetails: This is the user details that represents all of the auths that are valid for the query being made. The
 * requested auths will be reduced by this set of auths.
 */
public class AuthorizationsUtil {
    
    public static Authorizations union(Iterable<byte[]> authorizations1, Iterable<byte[]> authorizations2) {
        LinkedList<byte[]> aggregatedAuthorizations = Lists.newLinkedList();
        addTo(aggregatedAuthorizations, authorizations1);
        addTo(aggregatedAuthorizations, authorizations2);
        return new Authorizations(aggregatedAuthorizations);
    }
    
    protected static void addTo(LinkedList<byte[]> aggregatedAuthorizations, Iterable<byte[]> authsToAdd) {
        for (byte[] auth : authsToAdd) {
            aggregatedAuthorizations.add(auth);
        }
    }
    
    public static Set<Authorizations> mergeAuthorizations(String requestedAuths, Collection<? extends Collection<String>> userAuths) {
        HashSet<String> requested = null;
        if (!StringUtils.isEmpty(requestedAuths)) {
            requested = new HashSet<>(splitAuths(requestedAuths));
        }
        
        if (null == userAuths)
            return Collections.singleton(new Authorizations());
        
        HashSet<Authorizations> mergedAuths = new HashSet<>();
        HashSet<String> missingAuths = (requested == null) ? new HashSet<>() : new HashSet<>(requested);
        for (Collection<String> auths : userAuths) {
            if (null != requested) {
                missingAuths.removeAll(auths);
                auths = new HashSet<>(auths);
                auths.retainAll(requested);
            }
            
            mergedAuths.add(new Authorizations(auths.toArray(new String[auths.size()])));
        }
        
        if (!missingAuths.isEmpty()) {
            throw new IllegalArgumentException("User requested authorizations that they don't have. Missing: " + missingAuths + ", Requested: " + requested
                            + ", User: " + userAuths);
        }
        return mergedAuths;
    }
    
    public static DatawaveUser mergeAuths(DatawaveUser user, Set<String> auths) {
        return new DatawaveUser(user.getDn(), user.getUserType(), user.getEmail(), Sets.union(new HashSet<>(user.getAuths()), auths), user.getRoles(),
                        user.getRoleToAuthMapping(), user.getCreationTime(), user.getExpirationTime());
    }
    
    /**
     * Retrieves a set of "downgraded" authorizations. This will first validate that the requested auths are a subset of the {@code overallUserDetails} users's
     * authorizations. If not a subset then an {@link AuthorizationException} will be thrown. If a subset, then the requested auths will be subsequently reduced
     * by those in the {@code queryUserDetails} user's auths. The remaining proxy servers in the {@code queryUserDetails} will be returned as is for the
     * additional Authorizations in the list.
     *
     * @param requestedAuths
     *            The auths to use for the user's query. If this list contains any that are not owned by the {@code overallUserDetails} user, an
     *            {@link AuthorizationException} is thrown.
     * @param overallUserDetails
     *            The authorizations will be validated against this datawaveUserDetails's user's auths.
     * @param queryUserDetails
     *            The datawaveUserDetails from which to retrieve entity authorizations applicable to the query. The authorizations will be filtered by this
     *            prinipal's auths. This user's auths must be a subset of those in the {@code overallUserDetails} user's auths. If not then an
     *            {@link IllegalArgumentException} will be thrown.
     * @return A set of {@link Authorizations}, one per entity represented in {@code queryUserDetails}. The user's auths are replaced by {@code requestedAuths}
     *         intersected with the {@code queryUserDetails} user's auths so long as the {@code overallUserDetails} user actually had all of the auths. If
     *         {@code requestedAuths} is {@code null}, then the @{code queryUserDetails} user's auths are returned as-is.
     * @throws AuthorizationException
     *             if the requested auths is not a subset of the {@code overallUserDetails} users's auths
     * @throws IllegalArgumentException
     *             if the {@code queryUserDetails} user's auths are not a subset of the {@code overallUserDetails} user's auths
     */
    public static <T extends ProxiedUserDetails> Set<Authorizations> getDowngradedAuthorizations(String requestedAuths, T overallUserDetails,
                    T queryUserDetails) throws AuthorizationException {
        if (overallUserDetails == null || queryUserDetails == null) {
            return Collections.singleton(new Authorizations());
        }
        
        final UserAuthFunctions uaf = UserAuthFunctions.getInstance();
        final DatawaveUser queryUser = queryUserDetails.getPrimaryUser();
        
        // now return auths that are a reduced by what the query can handle.
        return uaf.mergeAuthorizations(getUserAuthorizations(requestedAuths, overallUserDetails, queryUserDetails), queryUserDetails.getProxiedUsers(),
                        u -> u != queryUser);
    }
    
    /**
     * Similar functionality to the above getDowngradedAuths, but returns in a String as opposed to a Set, and only returns the user's auths and not those for
     * any chained entity. This makes it easier to swap out queryParameters to use for createQueryAndNext(). Uses buildAuthorizationString to find the
     * authorizations the user has and compares those to the authorizations requested. Verifies that the user has access to the authorizations, and will return
     * the downgraded authorities if they are valid. If the request authorities they don't have, or request not authorizations, an exception is thrown.
     *
     * @param requestedAuths
     *            the requested downgrade authorizations
     * @param overallUserDetails
     *            The datawaveUserDetails from which to retrieve entity authorizations overall. The authorizations will be validated against this
     *            datawaveUserDetails's auths.
     * @param queryUserDetails
     *            The datawaveUserDetails from which to retrieve entity authorizations applicable to the query. The authorizations will be filtered by this
     *            prinipal's auths. The auths of this datawaveUserDetails must be a subset of the first datawaveUserDetails argument.
     * @return requested, unless the user represented by {@code overallUserDetails} does not have one or more of the auths in {@code requested}
     */
    public static <T extends ProxiedUserDetails> String downgradeUserAuths(String requestedAuths, T overallUserDetails, T queryUserDetails)
                    throws AuthorizationException {
        if (StringUtils.isEmpty(requestedAuths)) {
            throw new IllegalArgumentException("Requested authorizations must not be empty");
        }
        
        return getUserAuthorizations(requestedAuths, overallUserDetails, queryUserDetails).toString();
    }
    
    /**
     * Common functionality for the downgrading of user authorizations above.
     */
    protected static <T extends ProxiedUserDetails> Authorizations getUserAuthorizations(String requestedAuths, T overallUserDetails, T queryUserDetails)
                    throws AuthorizationException {
        final UserAuthFunctions uaf = UserAuthFunctions.getInstance();
        final DatawaveUser primaryUser = overallUserDetails.getPrimaryUser();
        final DatawaveUser queryUser = queryUserDetails.getPrimaryUser();
        
        // validate that the query user is actually a subset of the primary user
        if (!primaryUser.getAuths().containsAll(queryUser.getAuths())) {
            throw new IllegalArgumentException("System Error.  Unexpected authorization mismatch.  Please try again.");
        }
        
        // validate that the requestedAuths do not include anything outside of the datawaveUserDetails's auths
        uaf.validateRequestedAuthorizations(requestedAuths, primaryUser);
        
        // now return auths that are a reduced by what the query can handle.
        return uaf.getRequestedAuthorizations(requestedAuths, queryUser, false);
    }
    
    public static List<String> splitAuths(String requestedAuths) {
        return Arrays.asList(Iterables.toArray(Splitter.on(',').omitEmptyStrings().trimResults().split(requestedAuths), String.class));
    }
    
    public static Set<Authorizations> buildAuthorizations(Collection<? extends Collection<String>> userAuths) {
        if (null == userAuths) {
            return Collections.singleton(new Authorizations());
        }
        
        HashSet<Authorizations> auths = Sets.newHashSet();
        for (Collection<String> userAuth : userAuths) {
            auths.add(new Authorizations(userAuth.toArray(new String[userAuth.size()])));
        }
        
        return auths;
    }
    
    public static String buildAuthorizationString(Collection<? extends Collection<String>> userAuths) {
        if (null == userAuths) {
            return "";
        }
        
        HashSet<byte[]> b = new HashSet<>();
        for (Collection<String> userAuth : userAuths) {
            for (String string : userAuth) {
                b.add(string.getBytes(StandardCharsets.UTF_8));
            }
        }
        
        return new Authorizations(b).toString();
    }
    
    /**
     * Build the authorization string for a prinbcipal.
     *
     * @param datawaveUserDetails
     *            the datawaveUserDetails representing the user from which to generate authorizations
     * @return user authorizations string
     */
    public static String buildUserAuthorizationString(ProxiedUserDetails datawaveUserDetails) {
        String auths = "";
        if (datawaveUserDetails != null) {
            auths = new Authorizations(datawaveUserDetails.getPrimaryUser().getAuths().toArray(new String[0])).toString();
        }
        return auths;
    }
    
    public static Collection<Authorizations> minimize(Collection<Authorizations> authorizations) {
        return AuthorizationsMinimizer.minimize(authorizations);
    }
    
    public static Collection<? extends Collection<String>> prepareAuthsForMerge(Authorizations authorizations) {
        return Collections.singleton(new HashSet<>(Arrays.asList(authorizations.toString().split(","))));
    }
    
    /**
     * Merge datawaveUserDetailss. This can be used to create a composite view of a datawaveUserDetails when including remote systems
     *
     * @param proxiedUserDetails
     * @return The merge datawaveUserDetails
     */
    public static <T extends ProxiedUserDetails> T mergeProxiedUserDetails(T... proxiedUserDetails) {
        T mergedProxiedUserDetails = null;
        for (T userDetails : proxiedUserDetails) {
            if (mergedProxiedUserDetails == null) {
                mergedProxiedUserDetails = userDetails;
            } else {
                // verify we are merging like with like
                if (!mergedProxiedUserDetails.getPrimaryUser().getDn().equals(userDetails.getPrimaryUser().getDn())) {
                    throw new IllegalArgumentException("Cannot merge datawaveUserDetailss with different primary users: "
                                    + mergedProxiedUserDetails.getPrimaryUser().getDn() + " vs " + userDetails.getPrimaryUser().getDn());
                }
                
                // create a map of our users in the correct order
                LinkedHashMap<SubjectIssuerDNPair,DatawaveUser> users = mergedProxiedUserDetails.getProxiedUsers().stream()
                                .collect(Collectors.toMap(DatawaveUser::getDn, Function.identity(), (e1, e2) -> e1, LinkedHashMap::new));
                // keep track of extras
                LinkedHashMap<SubjectIssuerDNPair,DatawaveUser> extraProxies = new LinkedHashMap<>();
                
                // for each user, merge or add to extras
                for (DatawaveUser user : userDetails.getProxiedUsers()) {
                    if (users.containsKey(user.getDn())) {
                        users.put(user.getDn(), mergeUsers(users.get(user.getDn()), user));
                    } else {
                        extraProxies.put(user.getDn(), user);
                    }
                }
                
                // and create a merged datawaveUserDetails
                List<DatawaveUser> mergedUsers = new ArrayList<>(users.values());
                mergedUsers.addAll(extraProxies.values());
                mergedProxiedUserDetails = mergedProxiedUserDetails.newInstance(mergedUsers);
            }
        }
        return mergedProxiedUserDetails;
    }
    
    public static DatawaveUser mergeUsers(DatawaveUser... users) {
        DatawaveUser datawaveUser = null;
        for (DatawaveUser user : users) {
            if (datawaveUser == null) {
                datawaveUser = user;
            } else {
                // verify we are merging like with like.
                if (!user.getDn().equals(datawaveUser.getDn())) {
                    throw new IllegalArgumentException("Cannot merge different users: " + user.getDn() + " and " + datawaveUser.getDn());
                }
                if (!user.getUserType().equals(datawaveUser.getUserType())) {
                    throw new IllegalArgumentException("Cannot merge users of different types");
                }
                // merge role maps
                Multimap<String,String> mergedRoleToAuth = HashMultimap.create(datawaveUser.getRoleToAuthMapping());
                mergedRoleToAuth.putAll(user.getRoleToAuthMapping());
                // create merged user
                datawaveUser = new DatawaveUser(user.getDn(), user.getUserType(),
                                Sets.union(new HashSet<>(user.getAuths()), new HashSet<>(datawaveUser.getAuths())),
                                Sets.union(new HashSet<>(user.getRoles()), new HashSet<>(datawaveUser.getRoles())), mergedRoleToAuth,
                                System.currentTimeMillis());
            }
        }
        return datawaveUser;
    }
    
}
