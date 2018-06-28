package datawave.security.util;

import java.security.Principal;
import java.util.*;
import java.util.stream.Collectors;

import com.google.common.base.Splitter;
import com.google.common.collect.*;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.StringUtils;

import static java.nio.charset.StandardCharsets.UTF_8;

public class AuthorizationsUtil {
    public static Authorizations union(Iterable<byte[]> authorizations1, Iterable<byte[]> authorizations2) {
        LinkedList<byte[]> aggregatedAuthorizations = Lists.newLinkedList();
        addTo(aggregatedAuthorizations, authorizations1);
        addTo(aggregatedAuthorizations, authorizations2);
        return new Authorizations(aggregatedAuthorizations);
    }
    
    private static void addTo(LinkedList<byte[]> aggregatedAuthorizations, Iterable<byte[]> authsToAdd) {
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
            throw new IllegalArgumentException("User requested authorizations that they don't have. Missing: " + missingAuths.toString() + ", Requested: "
                            + requested + ", User: " + userAuths.toString());
        }
        return mergedAuths;
    }
    
    public static Set<Authorizations> getDowngradedAuthorizations(String requestedAuths, Principal principal) {
        if (principal instanceof DatawavePrincipal) {
            return getDowngradedAuthorizations(requestedAuths, (DatawavePrincipal) principal);
        } else {
            return Collections.singleton(new Authorizations());
        }
    }
    
    /**
     * Retrieves a set of "downgraded" authorizations. This retrieves all authorizations from {@code principal} and intersects the user auths (the
     * authorizations retrieved from {@code principal} for {@link DatawavePrincipal#getUserDN()}) with {@code requestedAuths}. All other entity auths retrieved
     * from {@code principal}, if any, are included in the result set as is. If {@code requestedAuths} contains any authorizations that are not in the user
     * auths list, then an {@link IllegalArgumentException} is thrown.
     *
     * @param requestedAuths
     *            The auths to use for the user's auths. If this list contains any that are not owned by the user, an {@link IllegalArgumentException} is
     *            thrown.
     * @param principal
     *            The principal from which to retrieve entity authorizations.
     * @return A set of {@link Authorizations}, one per entity represented in {@code principal}. The user's auths are replaced by {@code requestedAuths} so long
     *         as the user actually had all of the auths. If {@code requestedAuths} is {@code null}, then the user's auths are returned as-is.
     */
    public static LinkedHashSet<Authorizations> getDowngradedAuthorizations(String requestedAuths, DatawavePrincipal principal) {
        HashSet<String> requested = null;
        if (!StringUtils.isEmpty(requestedAuths)) {
            requested = new HashSet<>(splitAuths(requestedAuths));
        }
        
        LinkedHashSet<Authorizations> mergedAuths = new LinkedHashSet<>();
        
        if (null == principal) {
            mergedAuths.add(new Authorizations());
        } else {
            final DatawaveUser primaryUser = principal.getPrimaryUser();
            
            // Intersect the user's auths against the request auths and add that to the return map first.
            Collection<String> userAuths = downgradeUserAuths(primaryUser.getAuths(), requested);
            if (userAuths != null)
                mergedAuths.add(new Authorizations(userAuths.toArray(new String[userAuths.size()])));
            // Now simply add the auths from each non-primary user to the merged auths set
            // @formatter:off
            principal.getProxiedUsers().stream()
                    .filter(u -> u != primaryUser)
                    .map(DatawaveUser::getAuths)
                    .map(AuthorizationsUtil::toAuthorizations)
                    .forEach(mergedAuths::add);
            // @formatter:on
        }
        return mergedAuths;
    }
    
    private static Authorizations toAuthorizations(Collection<String> auths) {
        return new Authorizations(auths.stream().map(String::trim).map(s -> s.getBytes(UTF_8)).collect(Collectors.toList()));
    }
    
    private static Collection<String> downgradeUserAuths(Collection<String> userAuths, Collection<String> requestedAuths) {
        HashSet<String> downgradedAuths = (userAuths == null) ? new HashSet<String>() : new HashSet<>(userAuths);
        HashSet<String> missingAuths = (requestedAuths == null) ? new HashSet<String>() : new HashSet<>(requestedAuths);
        if (null != userAuths) {
            if (null != requestedAuths) {
                missingAuths.removeAll(userAuths);
                downgradedAuths.retainAll(requestedAuths);
            }
        }
        if (!missingAuths.isEmpty()) {
            throw new IllegalArgumentException("User requested authorizations that they don't have. Missing: " + missingAuths.toString() + ", Requested: "
                            + requestedAuths + ", User: " + userAuths);
        }
        return downgradedAuths;
    }
    
    /**
     * Similar functionality to the above getDowngradedAuths, but returns in a Stringas opposed to a Set, and only returns the user's auths and not those for
     * any chained entity. This makes it easier to swap out queryParameters to use for createQueryAndNext(). Uses buildAuthorizationString to find the
     * authorizations the user has and compares those to the authorizations requested. Verifies that the user has access to the authorizations, and will return
     * the downgraded authorities if they are valid. If the request authorities they don't have, or request not authorizations, an exception is thrown.
     *
     * @param principal
     *            the principal representing the user to verify that {@code requested} are all valid authorizations
     * @param requestedAuths
     *            the requested downgrade authorizations
     * @return requested, unless the user represented by {@code principal} does not have one or more of the auths in {@code requested}
     */
    public static String downgradeUserAuths(Principal principal, String requestedAuths) {
        
        List<String> requestedAuthsList = null;
        if (!StringUtils.isEmpty(requestedAuths)) {
            requestedAuthsList = splitAuths(requestedAuths);
        }
        
        // Find all authorizations the user has access to
        List<String> userAuthsList = null;
        String userAuths = buildUserAuthorizationString(principal);
        if (!StringUtils.isEmpty(userAuths)) {
            userAuthsList = splitAuths(userAuths);
        }
        
        Collection<String> downgradedAuths = downgradeUserAuths(userAuthsList, requestedAuthsList);
        
        if (!downgradedAuths.isEmpty())
            return AuthorizationsUtil.buildAuthorizationString(Collections.singletonList(downgradedAuths));
        else
            return "";
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
                b.add(string.getBytes());
            }
        }
        
        return new Authorizations(b).toString();
    }
    
    public static String buildUserAuthorizationString(Principal principal) {
        String auths = "";
        if (principal != null && (principal instanceof DatawavePrincipal)) {
            DatawavePrincipal datawavePrincipal = (DatawavePrincipal) principal;
            
            auths = new Authorizations(datawavePrincipal.getPrimaryUser().getAuths().toArray(new String[0])).toString();
        }
        return auths;
    }
    
    public static Collection<Authorizations> minimize(Collection<Authorizations> authorizations) {
        if (authorizations.size() > 1) {
            // check if all auth sets are subsets of a minimum set, and use just the minimum if so
            // minimize the sets to put all the common auths in the first one, and just the disjoint auths in the remainder.
            LinkedHashSet<TreeSet<String>> allAuths = new LinkedHashSet<>();
            for (Authorizations a : authorizations) {
                TreeSet<String> s = new TreeSet<>();
                for (byte[] b : a)
                    s.add(new String(b));
                allAuths.add(s);
            }
            
            Collection<? extends Collection<String>> minimized = minimize2(allAuths);
            authorizations = new LinkedHashSet<>(minimized.size());
            for (Collection<String> a : minimized)
                authorizations.add(new Authorizations(a.toArray(new String[a.size()])));
        }
        return authorizations;
    }
    
    public static Collection<? extends Collection<String>> minimize2(Collection<? extends Collection<String>> authorizations) {
        if (authorizations.size() > 1) {
            // check if all auth sets are subsets of a minimum set, and use just the minimum if so
            // minimize the sets to put all the common auths in the first one, and just the disjoint auths in the remainder.
            LinkedHashSet<TreeSet<String>> allAuths = new LinkedHashSet<>();
            for (Collection<String> a : authorizations)
                allAuths.add(new TreeSet<>(a));
            
            // Intersect all the auths to find the minimum auths of the group.
            TreeSet<String> minAuths = new TreeSet<>(allAuths.iterator().next());
            for (TreeSet<String> a : allAuths)
                minAuths.retainAll(a);
            
            boolean set = false;
            // Now see if any incoming auths equals the minimum set, and if so, just return that
            // Since a subset of everything will be the only one that can pass multiple visibility
            // expressions.
            for (TreeSet<String> a : allAuths) {
                if (minAuths.equals(a)) {
                    set = true;
                    authorizations = Collections.singleton(a);
                }
            }
            
            // If the hash set is smaller than the incoming auths list size, then we must have deduped and
            // can return a smaller set of auths.
            if (!set && authorizations.size() > allAuths.size()) {
                ArrayList<TreeSet<String>> newAuths = new ArrayList<>(allAuths.size());
                for (TreeSet<String> a : allAuths)
                    newAuths.add(a);
                authorizations = newAuths;
            }
        }
        return authorizations;
    }
    
    public static Collection<? extends Collection<String>> prepareAuthsForMerge(Authorizations authorizations) {
        return Collections.singleton(new HashSet<>(Arrays.asList(authorizations.toString().split(","))));
    }
}
