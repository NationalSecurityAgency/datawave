package datawave.security.util;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.security.Authorizations;

/**
 * @see AuthorizationsMinimizer#minimize(Collection)
 */
public class AuthorizationsMinimizer {
    
    /**
     * Given a collection of Accumulo {@link Authorizations} objects, this method attempts to compute a "minimum" set that can be used for a multi-entity scan
     * of data from Accumulo. The idea is that there is a call chain with multiple entities (e.g., a middleware broker acting on behalf of a GUI that is in turn
     * acting on behalf of a user). In order to ensure that only data visible to all of the entities is returned, we must add a visibility filter for each
     * entity in the chain to ensure that each key/value pair returned by Accumulo can be seen by all of the entities in the chain. Since we may be scanning a
     * very large number of keys, it would be ideal if we could reduce the number of visibility filters to a small set if that can be done safely.
     * <p>
     * The crux of the problem, and the reason we need multiple filters in the first place (as opposed to just intersecting the authorizations for all all
     * entities and using that set), is the fact that Accumulo visibility expressions may include disjunctions. Consider the following visibility expression.
     * <p>
     * {@code (A & B & ORG1) | (A & C & ORG2)}
     * <p>
     * If the incoming user has the Accumulo authorizations A, B, and ORG1 and the user is coming through a server with the Accumulo authorizations A, C, and
     * ORG2, then the server is allowed to see data protected with this expression and the user is also allowed, so the data should be returned with those two
     * calling entities. If we simply computed an intersection of the authorizations that set would be A. That set of authorizations does not match the
     * visibility expression and data that should be returned would not be.
     * <p>
     * Given that we must apply multiple visibility filters in order to properly return data for a calling entity chain, it is desirable to minimize the number
     * of visibility filters used, if possible. If a set of authorizations is a superset of any other set of authorizations, then it does not need to be tested.
     * As long as we are not changing any of the authorizations sets, we know we will never allow any data to be returned that would otherwise not be. By
     * removing authorizations sets, we might only prevent the return of data that should otherwise be returned. Consider a given set of authorizations that is
     * a subset of another. If the subset passes a visibility expression, then the superset will too since it contains at least the same authorizations, so
     * there is no need to test against it. If the subset does not pass, then the data is not visible to the chain anyway, so there is no point in testing the
     * superset.
     *
     * @param authorizations
     *            the list of Authorizations for each entity in the call chain
     * @return a minimized set of Authorizations that allows visibility of exactly the same data as {@code authorizations}
     */
    public static Collection<Authorizations> minimize(Collection<Authorizations> authorizations) {
        if (authorizations.size() > 1) {
            // Convert collection of Authorizations into a collection of String sets (the individual authorizations).
            // Since we are adding to a LinkedHashSet, this will de-dupe any duplicate authorization sets.
            final LinkedHashSet<Set<String>> allAuths = authorizations.stream()
                            .map(a -> a.getAuthorizations().stream().map(String::new).collect(Collectors.toCollection(HashSet::new)))
                            .collect(Collectors.toCollection(LinkedHashSet::new));
            
            // Go through the authorizations sets and remove any that are supersets of any other.
            for (Iterator<Set<String>> it = allAuths.iterator(); it.hasNext(); /* empty */) {
                Set<String> currentSet = it.next();
                if (allAuths.stream().filter(a -> a != currentSet && a.size() <= currentSet.size()).anyMatch(currentSet::containsAll))
                    it.remove();
            }
            
            // If we removed any sets of authorizations, then we need to convert the reduced set from
            // TreeSet<String> objects back into Authorizations objects.
            if (allAuths.size() < authorizations.size()) {
                authorizations = allAuths.stream().map(a -> new Authorizations(a.toArray(new String[0]))).collect(Collectors.toCollection(LinkedHashSet::new));
            }
        }
        return authorizations;
    }
}
