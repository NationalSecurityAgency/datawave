package datawave.query.planner.replacement.rules;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.LiteralRange;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.commons.jexl3.parser.ParserTreeConstants;

import java.util.HashMap;
import java.util.Map;

import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EVALUATION_ONLY;

public class RangeFieldReplacementRule implements FieldReplacementRule {
    private Map<String, String> fieldMap = new HashMap<>();

    public RangeFieldReplacementRule() {
    }

    public RangeFieldReplacementRule(Map<String, String> fieldMap) {
        this.fieldMap = fieldMap;
    }

    @Override
    public boolean matches(JexlNode node) {
        LiteralRange<?> range = JexlASTHelper.findRange().getRange(node);
        return range != null && fieldMap.containsKey(range.getFieldName());
    }

    @Override
    public void apply(JexlNode node) {
        LiteralRange<?> range = JexlASTHelper.findRange().getRange(node);

        if (range == null || !fieldMap.containsKey(range.getFieldName())) {
            return;
        }

        // Clone original range node and rename identifier in the clone
        JexlNode clonedLower = cloneAndReplaceField(range.getLowerNode(), fieldMap.get(range.getFieldName()));
        JexlNode clonedUpper = cloneAndReplaceField(range.getUpperNode(), fieldMap.get(range.getFieldName()));

        ASTAndNode newAnd = new ASTAndNode(ParserTreeConstants.JJTANDNODE);
        newAnd.jjtAddChild(clonedLower, 0);
        newAnd.jjtAddChild(clonedUpper, 1);

        // Mark the original node as "evaluationOnly"
        QueryPropertyMarker.create(node, EVALUATION_ONLY);

        // Replace the original node in-place with a new parent AND node
        ASTAndNode topLevel = new ASTAndNode(ParserTreeConstants.JJTANDNODE);
        topLevel.jjtAddChild(node, 0);
        topLevel.jjtAddChild(newAnd, 1);

        replaceNodeInParent(node, topLevel);
    }

    private JexlNode cloneAndReplaceField(JexlNode original, String newName) {
        try {
            JexlNode copy = JexlNodes.newInstanceOfType(original);

            for (int i = 0; i < original.jjtGetNumChildren(); i++) {
                JexlNode child = original.jjtGetChild(i);
                if (child instanceof ASTIdentifier) {
                    ASTIdentifier newId = JexlNodeFactory.buildIdentifier(newName);
                    copy.jjtAddChild(newId, i);
                } else {
                    copy.jjtAddChild(cloneAndReplaceField(child, newName), i);
                }
            }
            return copy;
        } catch (Exception e) {
            throw new RuntimeException("Failed to clone node: " + original.getClass(), e);
        }
    }

    private void replaceNodeInParent(JexlNode oldNode, JexlNode newNode) {
        JexlNode parent = oldNode.jjtGetParent();
        if (parent == null)
            return; // if root node, you’ll need to replace externally todo figure out root nodes

        for (int i = 0; i < parent.jjtGetNumChildren(); i++) {
            if (parent.jjtGetChild(i) == oldNode) {
                parent.jjtAddChild(newNode, i);
                newNode.jjtSetParent(parent);
                return;
            }
        }
    }

    public void setFieldMap(Map<String, String> fieldMap) {
        this.fieldMap = fieldMap;
    }

    public Map<String, String> getFieldMap() {
        return fieldMap;
    }
}
