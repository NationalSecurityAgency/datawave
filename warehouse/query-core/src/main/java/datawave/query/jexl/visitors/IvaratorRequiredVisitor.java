package datawave.query.jexl.visitors;

import static datawave.core.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_OR;
import static datawave.core.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_VALUE;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.JexlNode;

import datawave.core.query.jexl.nodes.QueryPropertyMarker;
import datawave.core.query.jexl.visitors.BaseVisitor;

/**
 * A visitor that checks the query tree to determine if the query requires an ivarator (ExceededValue or ExceededOr)
 *
 */
public class IvaratorRequiredVisitor extends BaseVisitor {

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
        if (instance.isAnyTypeOf(EXCEEDED_OR, EXCEEDED_VALUE)) {
            ivaratorRequired = true;
        } else if (!instance.isAnyTypeOf()) {
            super.visit(and, data);
        }
        return data;
    }
}
