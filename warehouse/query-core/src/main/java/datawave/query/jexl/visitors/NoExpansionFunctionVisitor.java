package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.FunctionJexlNodeVisitor;
import datawave.query.jexl.functions.QueryFunctions;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.JexlNode;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Visitor that parses out a NO_EXPANSION function and args from a query tree
 */
public class NoExpansionFunctionVisitor extends RebuildingVisitor {

    private final Set<String> noExpansionFields = new HashSet<>();

    /**
     * Extract a set of fields that will not be expanded by the {@link datawave.query.model.QueryModel}
     *
     * @param script
     *            the query tree
     * @return a set of fields, or an empty set if none exist
     */
    public static VisitResult findNoExpansionFields(ASTJexlScript script) {

        script = TreeFlatteningRebuildingVisitor.flatten(script);

        NoExpansionFunctionVisitor visitor = new NoExpansionFunctionVisitor();
        ASTJexlScript visited = (ASTJexlScript) script.jjtAccept(visitor, null);

        visited = TreeFlatteningRebuildingVisitor.flatten(visited);

        VisitResult result = new VisitResult();
        result.script = visited;
        result.noExpansionFields = visitor.getNoExpansionFields();

        return result;
    }

    private NoExpansionFunctionVisitor() {}

    public Set<String> getNoExpansionFields() {
        return this.noExpansionFields;
    }

    @Override
    public Object visit(ASTFunctionNode node, Object data) {

        FunctionJexlNodeVisitor visitor = new FunctionJexlNodeVisitor();
        node.jjtAccept(visitor, null);

        if (visitor.namespace().equals(QueryFunctions.QUERY_FUNCTION_NAMESPACE) && visitor.name().equalsIgnoreCase(QueryFunctions.NO_EXPANSION)) {

            noExpansionFields.addAll(visitor.args().stream().map(JexlASTHelper::getIdentifier).collect(Collectors.toSet()));

            // prune this query node by returning null
            return null;
        }
        return copy(node);
    }

    // methods to support pruning the function node from the tree

    @Override
    public Object visit(ASTAndNode node, Object data) {
        JexlNode visited = (JexlNode) super.visit(node, data);
        switch (visited.jjtGetNumChildren()) {
            case 0:
                return null;
            case 1:
                return visited.jjtGetChild(0);
            default:
                return visited;
        }
    }

    @Override
    public Object visit(ASTReference node, Object data) {
        JexlNode visited = (JexlNode) (super.visit(node, data));
        if (visited.jjtGetNumChildren() == 0) {
            return null;
        }
        return visited;
    }

    public static class VisitResult {
        public ASTJexlScript script;
        public Set<String> noExpansionFields;

        public VisitResult() {}
    }
}
