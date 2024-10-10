package datawave.query.jexl.visitors;

import static datawave.core.query.jexl.nodes.QueryPropertyMarker.MarkerType.BOUNDED_RANGE;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTGTNode;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTLTNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.log4j.Logger;

import datawave.core.common.logging.ThreadConfigurableLogger;
import datawave.core.query.jexl.nodes.QueryPropertyMarker;
import datawave.core.query.jexl.visitors.BaseVisitor;
import datawave.core.query.jexl.visitors.QueryPropertyMarkerVisitor;

/**
 * Count the number of terms where bounded ranges count as 1 term
 */
public class TermCountingVisitor extends BaseVisitor {
    private static final Logger log = ThreadConfigurableLogger.getLogger(TermCountingVisitor.class);

    public static int countTerms(JexlNode script) {
        TermCountingVisitor visitor = new TermCountingVisitor();

        return ((MutableInt) script.jjtAccept(visitor, new MutableInt(0))).intValue();
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {

        // Get safely
        QueryPropertyMarker.Instance instance = QueryPropertyMarkerVisitor.getInstance(node);
        if (instance.isType(BOUNDED_RANGE)) {
            // count each bounded range as 1
            ((MutableInt) data).increment();
        } else if (instance.isIvarator()) {
            ((MutableInt) data).increment();
        } else {
            // otherwise recurse on the children
            super.visit(node, data);
        }

        return data;
    }

    @Override
    public Object visit(ASTEQNode node, Object data) {
        ((MutableInt) data).increment();
        return data;
    }

    @Override
    public Object visit(ASTNENode node, Object data) {
        ((MutableInt) data).increment();
        return data;
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        ((MutableInt) data).increment();
        return data;
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
        ((MutableInt) data).increment();
        return data;
    }

    @Override
    public Object visit(ASTLTNode node, Object data) {
        ((MutableInt) data).increment();
        return data;
    }

    @Override
    public Object visit(ASTLENode node, Object data) {
        ((MutableInt) data).increment();
        return data;
    }

    @Override
    public Object visit(ASTGTNode node, Object data) {
        ((MutableInt) data).increment();
        return data;
    }

    @Override
    public Object visit(ASTGENode node, Object data) {
        ((MutableInt) data).increment();
        return data;
    }

    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        ((MutableInt) data).increment();
        return data;
    }

}
