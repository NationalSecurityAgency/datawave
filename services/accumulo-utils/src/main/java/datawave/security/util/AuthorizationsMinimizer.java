package datawave.security.util;

import org.apache.accumulo.core.security.Authorizations;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.TreeSet;

public class AuthorizationsMinimizer {
    
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
    
    private static Collection<? extends Collection<String>> minimize2(Collection<? extends Collection<String>> authorizations) {
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
                newAuths.addAll(allAuths);
                authorizations = newAuths;
            }
        }
        return authorizations;
    }
}
