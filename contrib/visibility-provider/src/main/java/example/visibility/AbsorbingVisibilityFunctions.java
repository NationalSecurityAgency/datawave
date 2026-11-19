package example.visibility;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.access.ParsedAccessExpression.ExpressionType.AND;
import static org.apache.accumulo.access.ParsedAccessExpression.ExpressionType.AUTHORIZATION;
import static org.apache.accumulo.access.ParsedAccessExpression.ExpressionType.OR;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.access.ParsedAccessExpression;
import org.apache.accumulo.core.security.ColumnVisibility;

import datawave.visibility.AccessExpressionUtil;
import datawave.visibility.FlattenedVisibilityCache;
import datawave.visibility.VisibilityFunctions;

/** An example downstream policy: apply the Boolean absorption law without assuming any hierarchy among authorization labels. */
public final class AbsorbingVisibilityFunctions implements VisibilityFunctions {
    @Override
    public ColumnVisibility combine(Collection<ColumnVisibility> visibilities) {
        ColumnVisibility combined = AccessExpressionUtil.conjunction(visibilities);
        ParsedAccessExpression root = AccessExpressionUtil.getAccess().newParsedExpression(new String(combined.getExpression(), UTF_8));
        if (root.getType() != AND) {
            return combined;
        }

        List<ParsedAccessExpression> terms = root.getChildren();
        Set<String> requiredLabels = terms.stream().filter(term -> term.getType() == AUTHORIZATION).map(ParsedAccessExpression::getExpression)
                        .collect(Collectors.toSet());
        if (requiredLabels.isEmpty()) {
            return combined;
        }

        // In A & (A | B), A already satisfies the OR. A nested occurrence such as ((A & C) | B) is not sufficient.
        List<ParsedAccessExpression> retained = new ArrayList<>(terms);
        boolean changed = retained.removeIf(term -> term.getType() == OR && term.getChildren().stream().anyMatch(
                        alternative -> alternative.getType() == AUTHORIZATION && requiredLabels.contains(alternative.getExpression())));
        if (!changed) {
            return combined;
        }
        return AccessExpressionUtil.conjunction(retained.stream().map(term -> new ColumnVisibility(term.getExpression())).collect(Collectors.toList()));
    }

    @Override
    public byte[] flatten(ColumnVisibility visibility) {
        return FlattenedVisibilityCache.flatten(visibility);
    }
}
