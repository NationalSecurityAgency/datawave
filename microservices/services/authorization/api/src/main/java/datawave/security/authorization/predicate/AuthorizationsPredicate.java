package datawave.security.authorization.predicate;

import java.util.function.Predicate;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.VisibilityEvaluator;
import org.apache.accumulo.core.security.VisibilityParseException;

/**
 * This is a predicate that will test the auths against a specified visibility (as defined by accumulo's ColumnVisibility). In addition to the visibility, one
 * can specify that only the first of the authorizations is matched (presumably the user).
 */
public class AuthorizationsPredicate implements Predicate<Authorizations> {
    
    // A visibility string to be matched against the auths being used for the query
    private ColumnVisibility visibility;
    
    public AuthorizationsPredicate() {}
    
    public AuthorizationsPredicate(String visibility) {
        setVisibility(visibility);
    }
    
    @Override
    public boolean test(Authorizations auths) {
        // match the visibility against the auths.
        ColumnVisibility vis = getVisibility();
        VisibilityEvaluator ve = new VisibilityEvaluator(auths);
        try {
            return (ve.evaluate(vis));
        } catch (VisibilityParseException e) {
            throw new RuntimeException(e);
        }
    }
    
    public ColumnVisibility getVisibility() {
        return visibility;
    }
    
    public void setVisibility(ColumnVisibility visibility) {
        this.visibility = visibility;
    }
    
    public void setVisibility(String visibility) {
        setVisibility(new ColumnVisibility(visibility));
    }
    
    @Override
    public String toString() {
        return "(auths =~ " + visibility + ')';
    }
}
