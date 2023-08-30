package datawave.query.jexl.visitors;

import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTUnsatisfiableGENode;
import org.apache.commons.jexl2.parser.ASTUnsatisfiableGTNode;
import org.apache.commons.jexl2.parser.ASTUnsatisfiableLENode;
import org.apache.commons.jexl2.parser.ASTUnsatisfiableLTNode;
import org.apache.commons.jexl2.parser.JexlNode;

/**
 *
 */
public class RangeExpansionThresholdRebuildingVisitor extends RebuildingVisitor {

    public static JexlNode copy(JexlNode root) {
        RangeExpansionThresholdRebuildingVisitor visitor = new RangeExpansionThresholdRebuildingVisitor();

        return (JexlNode) root.jjtAccept(visitor, null);
    }

    @Override
    public Object visit(ASTGTNode node, Object data) {
        return RebuildingVisitor.copyInto(node, ASTUnsatisfiableGTNode.create());
    }

    @Override
    public Object visit(ASTGENode node, Object data) {
        return RebuildingVisitor.copyInto(node, ASTUnsatisfiableGENode.create());
    }

    @Override
    public Object visit(ASTLTNode node, Object data) {
        return RebuildingVisitor.copyInto(node, ASTUnsatisfiableLTNode.create());
    }

    @Override
    public Object visit(ASTLENode node, Object data) {
        return RebuildingVisitor.copyInto(node, ASTUnsatisfiableLENode.create());
    }
}
