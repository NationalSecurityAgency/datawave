package datawave.query.planner.rules;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.JexlNode;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.jexl.visitors.RebuildingVisitor;
import datawave.query.util.MetadataHelper;

/**
 * Push certain regex terms down to evaluation, thus skipping index expansion and field index execution
 * <p>
 * Note: cannot configure index only fields
 */
public class EvaluationOnlyPushdownRule implements NodeTransformRule {

    private final Set<FieldPattern> fieldPatterns = new HashSet<>();

    @Override
    public JexlNode apply(JexlNode node, ShardQueryConfiguration config, MetadataHelper helper) {
        if (node instanceof ASTERNode) {
            String field = JexlASTHelper.getIdentifier(node);
            String pattern = String.valueOf(JexlASTHelper.getLiteralValue(node));
            if (fieldPatterns.contains(new FieldPattern(field, pattern))) {
                JexlNode copy = RebuildingVisitor.copy(node);
                return QueryPropertyMarker.create(copy, QueryPropertyMarker.MarkerType.EVALUATION_ONLY);
            }
        }
        return node;
    }

    public Set<FieldPattern> getFieldPatterns() {
        return fieldPatterns;
    }

    public void setFieldPatterns(Set<FieldPattern> fieldPatterns) {
        this.fieldPatterns.addAll(fieldPatterns);
    }
}
