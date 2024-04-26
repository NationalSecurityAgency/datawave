package datawave.query.jexl.visitors;

import datawave.query.jexl.nodes.QueryPropertyMarker;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.JexlNode;

import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EVALUATION_ONLY;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_OR;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_TERM;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_VALUE;

/**
 * A visitor that checks the query tree to determine if the query requires an ivarator (ExceededValue, ExceededTerm, or ExceededOr)
 *
 */
public class IvaratorRequiredVisitor extends ShortCircuitBaseVisitor {

    private boolean ivaratorRequired = false;

    public boolean isIvaratorRequired() {
        return ivaratorRequired;
    }

    public static boolean isIvaratorRequired(JexlNode node) {
        IvaratorRequiredVisitor visitor = new IvaratorRequiredVisitor();
        node.jjtAccept(visitor, null);
        return visitor.isIvaratorRequired();
    }

    @Override
    public Object visit(ASTAndNode and, Object data) {
        QueryPropertyMarker.Instance instance = QueryPropertyMarker.findInstance(and);
        if (instance.isType(EVALUATION_ONLY)) {
            return data;
        } else if (instance.isAnyTypeOf(EXCEEDED_OR, EXCEEDED_VALUE, EXCEEDED_TERM)) {
            ivaratorRequired = true;
        } else if (!instance.isAnyTypeOf()) {
            super.visit(and, data);
        }
        return data;
    }
}
