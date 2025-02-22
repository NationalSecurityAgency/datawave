package datawave.marking;

import java.nio.charset.Charset;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.ColumnVisibility.Node;
import org.apache.accumulo.core.security.ColumnVisibility.NodeType;
import org.apache.accumulo.core.security.VisibilityEvaluator;
import org.apache.accumulo.core.security.VisibilityParseException;
import org.apache.log4j.Logger;

import datawave.marking.MarkingFunctions.Exception;

public class ColumnVisibilityHelper {
    
    protected static final Charset charset = Charset.forName("UTF-8");
    private static Logger log = Logger.getLogger(ColumnVisibilityHelper.class);
    
    public static ColumnVisibility simplifyColumnVisibilityForAuthorizations(ColumnVisibility columnVisibility, Collection<Authorizations> authorizations)
                    throws MarkingFunctions.Exception {
        
        ColumnVisibility simplifiedCV = columnVisibility;
        
        Node node = columnVisibility.getParseTree();
        if (node.getType() == NodeType.OR) {
            if (log.isTraceEnabled()) {
                log.trace("Top level OR Node, removing unsatisfied branches from: " + columnVisibility);
            }
            byte[] expression = columnVisibility.getExpression();
            if (authorizations != null) {
                HashSet<VisibilityEvaluator> ve = new HashSet<>();
                for (Authorizations a : authorizations)
                    ve.add(new VisibilityEvaluator(a));
                removeUnsatisfiedTopLevelOrNodes(ve, expression, node);
            }
            
            simplifiedCV = ColumnVisibilityHelper.flatten(node, expression);
            if (log.isTraceEnabled()) {
                log.trace("removed unsatisfied branches, visibility now: " + simplifiedCV);
            }
        }
        return simplifiedCV;
    }
    
    public static ColumnVisibility removeUndisplayedVisibilities(ColumnVisibility columnVisibility, Set<String> undisplayedVisibilities)
                    throws MarkingFunctions.Exception {
        ColumnVisibility newColumnVisibility = columnVisibility;
        if (undisplayedVisibilities != null && !undisplayedVisibilities.isEmpty()) {
            byte[] expression = columnVisibility.getExpression();
            Node node = columnVisibility.getParseTree();
            removeUndisplayedVisibilities(node, expression, undisplayedVisibilities);
            newColumnVisibility = flatten(node, expression);
        }
        return newColumnVisibility;
    }
    
    private static void removeUnsatisfiedTopLevelOrNodes(Set<VisibilityEvaluator> ve, byte[] expression, Node node) throws Exception {
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
    
    private static boolean isUnsatisfied(int position, Set<VisibilityEvaluator> ve, Node currNode, byte[] expression) throws Exception {
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
    
    private static String termNodeToString(Node termNode, byte[] expression) throws Exception {
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
    
    private static ColumnVisibility flatten(Node node, byte[] expression) {
        
        Node newNode = ColumnVisibility.normalize(node, expression);
        StringBuilder sb = new StringBuilder();
        ColumnVisibility.stringify(newNode, expression, sb);
        return new ColumnVisibility(sb.toString());
    }
    
    private static void removeUndisplayedVisibilities(Node node, byte[] expression, Set<String> undisplayedVisibilities) throws MarkingFunctions.Exception {
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
                if (currNode.getChildren().isEmpty()) {
                    children.remove(x);
                }
            }
        }
    }
}
