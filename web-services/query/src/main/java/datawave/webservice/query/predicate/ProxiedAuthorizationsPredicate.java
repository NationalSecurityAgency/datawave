package datawave.webservice.query.predicate;

import java.util.List;
import java.util.function.Predicate;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;

/**
 * This is a predicate that will test the auths against a specified visibility (as defined by accumulo's ColumnVisibility). In addition to the visibility, one
 * can specify that only the first of the authorizations is matched (presumably the user).
 */
public class ProxiedAuthorizationsPredicate implements Predicate<List<Authorizations>> {

    public static enum MatchType {
        // match only the first of the Authorizations (presumably the user)
        FIRST,
        // match all of the Authorizations (the user and proxies)
        ALL,
    }

    private MatchType matchType = MatchType.ALL;

    private final AuthorizationsPredicate authsTest = new AuthorizationsPredicate();

    public ProxiedAuthorizationsPredicate() {}

    public ProxiedAuthorizationsPredicate(String visibility) {
        setVisibility(visibility);
    }

    public ProxiedAuthorizationsPredicate(String visibility, MatchType matchType) {
        this(visibility);
        setMatchType(matchType);
    }

    @Override
    public boolean test(List<Authorizations> auths) {
        // match the visibility against the auths.
        for (Authorizations auth : auths) {
            if (authsTest.test(auth)) {
                // if only matching the first one, then return true immediately
                if (matchType == MatchType.FIRST) {
                    return true;
                }
            } else {
                return false;
            }
        }
        return true;
    }

    public MatchType getMatchType() {
        return matchType;
    }

    public void setMatchType(MatchType matchType) {
        this.matchType = matchType;
    }

    public ColumnVisibility getVisibility() {
        return authsTest.getVisibility();
    }

    public void setVisibility(ColumnVisibility visibility) {
        authsTest.setVisibility(visibility);
    }

    public void setVisibility(String visibility) {
        authsTest.setVisibility(visibility);
    }

}
