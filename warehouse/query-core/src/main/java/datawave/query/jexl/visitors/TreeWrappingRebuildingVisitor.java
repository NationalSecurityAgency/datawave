package datawave.query.jexl.visitors;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParserTreeConstants;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import datawave.query.jexl.JexlNodeFactory;

public class TreeWrappingRebuildingVisitor extends RebuildingVisitor {

    private static final Logger log = LogManager.getLogger(TreeWrappingRebuildingVisitor.class);

    public static ASTJexlScript wrap(JexlNode node) {
        TreeWrappingRebuildingVisitor visitor = new TreeWrappingRebuildingVisitor();

        return (ASTJexlScript) node.jjtAccept(visitor, null);
    }

    @Override
    public Object visit(ASTOrNode node, Object data) {
        ASTOrNode newNode = new ASTOrNode(ParserTreeConstants.JJTORNODE);
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            JexlNode child = (JexlNode) node.jjtGetChild(i).jjtAccept(this, data);
            newNode.jjtAddChild(child, newNode.jjtGetNumChildren());
            child.jjtSetParent(newNode);
        }
        return JexlNodeFactory.wrap(newNode);
    }
}
