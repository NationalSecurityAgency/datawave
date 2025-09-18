package datawave.query.planner.replacement.rules;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.LiteralRange;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.jexl.visitors.RebuildingVisitor;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
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
    public JexlNode apply(JexlNode node) {
        LiteralRange<?> range = JexlASTHelper.findRange().getRange(node);

        if (range == null || !fieldMap.containsKey(range.getFieldName())) {
            return node;
        }

        // Create a copy and mark it as "evaluationOnly"
        JexlNode copy = RebuildingVisitor.copy(node);
        JexlNode evalNode = QueryPropertyMarker.create(copy, EVALUATION_ONLY);

        // rename the field for the range nodes
        replaceField(range.getLowerNode(), fieldMap.get(range.getFieldName()));
        replaceField(range.getUpperNode(), fieldMap.get(range.getFieldName()));

        // Create a Reference Expression for the original node and top level AND for both the ref and the eval node
        ASTReferenceExpression ref = JexlNodes.makeRefExp();
        ASTAndNode topLevel = new ASTAndNode(ParserTreeConstants.JJTANDNODE);
        node.jjtSetParent(ref);
        ref.jjtSetParent(topLevel);
        evalNode.jjtSetParent(topLevel);
        ref.jjtAddChild(node, 0);
        topLevel.jjtAddChild(evalNode, 0);
        topLevel.jjtAddChild(ref, 1);

        return topLevel;
    }

    private void replaceField(JexlNode node, String newName) {
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            JexlNode child = node.jjtGetChild(i);
            if (child instanceof ASTIdentifier) {
                ASTIdentifier newId = JexlNodeFactory.buildIdentifier(newName);
                node.jjtAddChild(newId, i);
                newId.jjtSetParent(node);
            } else {
                replaceField(child, newName);
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
