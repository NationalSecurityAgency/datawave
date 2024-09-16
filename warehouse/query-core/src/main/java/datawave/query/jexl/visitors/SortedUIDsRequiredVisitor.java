package datawave.query.jexl.visitors;

import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.DELAYED;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_OR;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_VALUE;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.INDEX_HOLE;

import java.util.Set;

import org.apache.commons.jexl3.parser.ASTAssignment;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.ASTMethodNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.ASTNotNode;
import org.apache.commons.jexl3.parser.ASTReference;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.log4j.Logger;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.nodes.QueryPropertyMarker;

/**
 * A visitor that checks the query tree to determine if sorting the UIDs coming from the field index is required. Normally they are, however if the query
 * contains only one ivarator and no other indexed terms, then there is no need to sort.
 *
 */
public class SortedUIDsRequiredVisitor extends BaseVisitor {
    private static final Logger log = Logger.getLogger(SortedUIDsRequiredVisitor.class);

    private final Set<String> indexedFields;
    private int indexedFieldCount = 0;
    private int negatedIndexedFieldCount = 0;
    private int ivarators = 0;
    private boolean negated = false;
    private boolean acknowledgeDelayedPredicates = false;

    public boolean areSortedUIDsRequired() {
        return (ivarators != 1 || indexedFieldCount > 1 || negatedIndexedFieldCount > 0);
    }

    public SortedUIDsRequiredVisitor(Set<String> indexedFields, boolean acknowledgeDelayedPredicates) {
        this.indexedFields = indexedFields;
        this.acknowledgeDelayedPredicates = acknowledgeDelayedPredicates;
    }

    public static boolean isRequired(JexlNode tree, Set<String> indexedFields, boolean acknowledgeDelayedPredicates) {
        SortedUIDsRequiredVisitor visitor = new SortedUIDsRequiredVisitor(indexedFields, acknowledgeDelayedPredicates);
        tree.jjtAccept(visitor, null);
        return visitor.areSortedUIDsRequired();
    }

    @Override
    public Object visit(ASTNENode node, Object data) {
        negated = !negated;
        Object rtrn = super.visit(node, data);
        negated = !negated;
        return rtrn;
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
        negated = !negated;
        Object rtrn = super.visit(node, data);
        negated = !negated;
        return rtrn;
    }

    @Override
    public Object visit(ASTNotNode node, Object data) {
        negated = !negated;
        Object rtrn = super.visit(node, data);
        negated = !negated;
        return rtrn;
    }

    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        // function nodes are not run against the index
        return data;
    }

    @Override
    public Object visit(ASTMethodNode node, Object data) {
        // function nodes are not run against the index
        return data;
    }

    @Override
    public Object visit(ASTReference node, Object data) {
        // delayed predicates are not run against the index (if acknowledging them)
        QueryPropertyMarker.Instance instance = QueryPropertyMarker.findInstance(node);
        if (!(acknowledgeDelayedPredicates && instance.isType(DELAYED))) {
            if (!instance.isType(INDEX_HOLE)) {
                if (instance.isAnyTypeOf(EXCEEDED_OR, EXCEEDED_VALUE)) {
                    ivarators++;
                    indexedFieldCount++;
                } else {
                    data = super.visit(node, data);
                }
            }
        }
        return data;
    }

    @Override
    public Object visit(ASTIdentifier node, Object data) {
        countIndexed(node);
        return data;
    }

    @Override
    public Object visit(ASTAssignment node, Object data) {
        // assignment nodes are not run against the index
        return data;
    }

    protected void countIndexed(ASTIdentifier node) {
        if (isIndexed(node)) {
            if (negated) {
                negatedIndexedFieldCount++;
            } else {
                indexedFieldCount++;
            }
        }
    }

    protected boolean isIndexed(ASTIdentifier node) {
        final String fieldName = JexlASTHelper.deconstructIdentifier(node.getName());
        return this.indexedFields.contains(fieldName);
    }

}
