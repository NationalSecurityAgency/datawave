package datawave.query.jexl.visitors;

import datawave.query.jexl.nodes.ExceededOrThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededValueThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.log4j.Logger;

/**
 * A visitor that checks the query tree to determine if the query requires an ivarator (ExceededValue or ExceededOr)
 * 
 */
public class IvaratorRequiredVisitor extends BaseVisitor {
    private static final Logger log = Logger.getLogger(IvaratorRequiredVisitor.class);
    
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
        
        if (ExceededOrThresholdMarkerJexlNode.instanceOf(and) || ExceededValueThresholdMarkerJexlNode.instanceOf(and)) {
            ivaratorRequired = true;
        } else if (!QueryPropertyMarker.instanceOf(and, null)) {
            super.visit(and, data);
        }
        return data;
    }
}
