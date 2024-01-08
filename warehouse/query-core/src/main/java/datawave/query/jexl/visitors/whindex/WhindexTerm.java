package datawave.query.jexl.visitors.whindex;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.RebuildingVisitor;

/**
 * This class contains all of the required components necessary to create a whindex term.
 */
class WhindexTerm {
    private final JexlNode fieldValueNode;
    private final JexlNode mappableNode;
    private final String newFieldName;

    public WhindexTerm(JexlNode fieldValueNode, JexlNode mappableNode, String newFieldName) {
        this.fieldValueNode = fieldValueNode;
        this.mappableNode = mappableNode;
        this.newFieldName = newFieldName;
    }

    public JexlNode getFieldValueNode() {
        return fieldValueNode;
    }

    public JexlNode getMappableNode() {
        return mappableNode;
    }

    public String getNewFieldName() {
        return newFieldName;
    }

    public JexlNode createWhindexNode() {
        JexlNode copiedNode = RebuildingVisitor.copy(mappableNode);
        List<ASTIdentifier> identifiers = JexlASTHelper.getIdentifiers(copiedNode);
        Set<String> fieldNames = new HashSet<>();

        identifiers.forEach(x -> fieldNames.add(JexlASTHelper.getIdentifier(x)));

        if (fieldNames.size() == 1) {
            for (ASTIdentifier identifier : identifiers) {
                JexlNodes.replaceChild(identifier.jjtGetParent(), identifier, JexlNodes.makeIdentifier(newFieldName));
            }
        } else {
            throw new RuntimeException("Cannot create a Whindex term using multiple identifiers.");
        }

        return copiedNode;
    }

    public boolean contains(JexlNode node) {
        // @formatter:off
        return fieldValueNode == node || JexlASTHelper.dereference(fieldValueNode) == node ||
                mappableNode == node || JexlASTHelper.dereference(mappableNode) == node;
        // @formatter:on
    }
}
