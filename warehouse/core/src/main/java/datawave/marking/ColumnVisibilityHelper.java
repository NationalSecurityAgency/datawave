package datawave.marking;

import java.nio.charset.Charset;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import datawave.marking.MarkingFunctions.Exception;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.ColumnVisibility.Node;
import org.apache.accumulo.core.security.ColumnVisibility.NodeType;
import org.apache.accumulo.core.security.VisibilityEvaluator;
import org.apache.accumulo.core.security.VisibilityParseException;
import org.apache.log4j.Logger;

public class ColumnVisibilityHelper {
    
    static protected final Charset charset = Charset.forName("UTF-8");
    static private Logger log = Logger.getLogger(ColumnVisibilityHelper.class);
    
    static public ColumnVisibility simplifyColumnVisibilityForAuthorizations(ColumnVisibility columnVisibility, Collection<Authorizations> authorizations)
                    throws MarkingFunctions.Exception {
        
        ColumnVisibility simplifiedCV = columnVisibility;
        
        Node node = columnVisibility.getParseTree();
        if (node.getType() == NodeType.OR) {
            if (log.isTraceEnabled()) {
                log.trace("Top level OR Node, removing unsatisfied branches from: " + columnVisibility.toString());
            }
            byte[] expression = columnVisibility.getExpression();
            if (authorizations != null) {
                HashSet<VisibilityEvaluator> ve = new HashSet<VisibilityEvaluator>();
                for (Authorizations a : authorizations)
                    ve.add(new VisibilityEvaluator(a));
                removeUnsatisfiedTopLevelOrNodes(ve, expression, node);
            }
            
            simplifiedCV = ColumnVisibilityHelper.flatten(node, expression);
            if (log.isTraceEnabled()) {
                log.trace("removed unsatisfied branches, visibility now: " + simplifiedCV.toString());
            }
        }
        return simplifiedCV;
    }
    
    static public ColumnVisibility removeUndisplayedVisibilities(ColumnVisibility columnVisibility, Set<String> undisplayedVisibilities)
                    throws MarkingFunctions.Exception {
        ColumnVisibility newColumnVisibility = columnVisibility;
        if (undisplayedVisibilities != null && undisplayedVisibilities.size() > 0) {
            byte[] expression = columnVisibility.getExpression();
            Node node = columnVisibility.getParseTree();
            removeUndisplayedVisibilities(node, expression, undisplayedVisibilities);
            newColumnVisibility = flatten(node, expression);
        }
        return newColumnVisibility;
    }
    
    static private void removeUnsatisfiedTopLevelOrNodes(Set<VisibilityEvaluator> ve, byte[] expression, Node node) throws Exception {
        if (node.getType() == NodeType.OR) {
            List<Node> children = node.getChildren();
            int lastNode = children.size() - 1;
            for (int x = lastNode; x >= 0; x--) {
                Node currNode = children.get(x);
                boolean remove = isUnsatisfied(x, ve, currNode, expression);
                if (remove == true) {
                    children.remove(x);
                }
            }
        }
    }
    
    static private boolean isUnsatisfied(int position, Set<VisibilityEvaluator> ve, Node currNode, byte[] expression) throws Exception {
        boolean unsatisfied = false;
        try {
            ColumnVisibility currVis = ColumnVisibilityHelper.flatten(currNode, expression);
            for (VisibilityEvaluator v : ve) {
                if (!v.evaluate(currVis))
                    unsatisfied = true;
            }
        } catch (VisibilityParseException e) {
            throw new MarkingFunctions.Exception(e);
        }
        return unsatisfied;
    }
    
    static private String termNodeToString(Node termNode, byte[] expression) throws Exception {
        int start = termNode.getTermStart();
        int end = termNode.getTermEnd();
        
        String str = "[ERROR]";
        try {
            str = new String(expression, start, end - start, charset);
        } catch (RuntimeException e) {
            log.error("Error converting term: start:" + start + " length:" + (end - start) + "of expression:" + expression + " -- " + e.getMessage());
            throw new MarkingFunctions.Exception(e);
        }
        return str;
    }
    
    static private ColumnVisibility flatten(Node node, byte[] expression) {
        
        Node newNode = ColumnVisibility.normalize(node, expression);
        StringBuilder sb = new StringBuilder();
        ColumnVisibility.stringify(newNode, expression, sb);
        return new ColumnVisibility(sb.toString());
    }
    
    static private void removeUndisplayedVisibilities(Node node, byte[] expression, Set<String> undisplayedVisibilities) throws MarkingFunctions.Exception {
        List<Node> children = node.getChildren();
        // walk backwards so we don't change the index that we need to remove
        int lastNode = children.size() - 1;
        for (int x = lastNode; x >= 0; x--) {
            Node currNode = children.get(x);
            if (currNode.getType() == NodeType.TERM) {
                String nodeString = termNodeToString(currNode, expression);
                if (undisplayedVisibilities.contains(nodeString)) {
                    children.remove(x);
                }
            } else {
                removeUndisplayedVisibilities(currNode, expression, undisplayedVisibilities);
                // if all children of this NodeType.AND or NodeType.OR node have been removed, remove the node itself.
                if (currNode.getChildren().size() == 0) {
                    children.remove(x);
                }
            }
        }
    }
}
