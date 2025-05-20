package datawave.query.jexl.visitors;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.JexlNode;

/**
 * Determine the depth of the query (nested and, or, or parens) up to maxDepth+1.
 *
 */
public class DepthVisitor extends BaseVisitor {
    private int maxDepth = 100;

    public DepthVisitor(int maxDepth) {
        this.maxDepth = maxDepth;
    }

    public static class Depth {
        int currentDepth = 0;
        int maxDepth = 0;

        public int dive() {
            currentDepth++;
            if (currentDepth > maxDepth) {
                maxDepth = currentDepth;
            }
            return currentDepth;
        }

        public void rise() {
            currentDepth--;
        }

        public int getMaxDepth() {
            return maxDepth;
        }
    }

    /**
     * Determine the depth of the query (nested and, or, or parens) up to maxDepth+1.
     *
     * @param maxDepth
     *            the max depth
     * @param root
     *            the root node
     * @return the query depth
     */
    public static int getDepth(JexlNode root, int maxDepth) {
        DepthVisitor vis = new DepthVisitor(maxDepth);
        Depth depth = new Depth();
        root.jjtAccept(vis, depth);
        return depth.getMaxDepth();
    }

    @Override
    public Object visit(ASTJexlScript node, Object data) {
        int depth = ((Depth) data).dive();
        try {
            // short circuit
            if (depth > maxDepth) {
                return data;
            }
            return super.visit(node, data);
        } finally {
            ((Depth) data).rise();
        }
    }

    @Override
    public Object visit(ASTOrNode node, Object data) {
        int depth = ((Depth) data).dive();
        try {
            // short circuit
            if (depth > maxDepth) {
                return data;
            }
            return super.visit(node, data);
        } finally {
            ((Depth) data).rise();
        }
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {
        int depth = ((Depth) data).dive();
        try {
            // short circuit
            if (depth > maxDepth) {
                return data;
            }
            return super.visit(node, data);
        } finally {
            ((Depth) data).rise();
        }
    }

    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        int depth = ((Depth) data).dive();
        try {
            // short circuit
            if (depth > maxDepth) {
                return data;
            }
            return super.visit(node, data);
        } finally {
            ((Depth) data).rise();
        }
    }

}
