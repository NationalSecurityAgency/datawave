package datawave.marking;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Arrays;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.ColumnVisibility.Node;
import org.apache.accumulo.core.security.ColumnVisibility.NodeComparator;
import org.apache.accumulo.core.security.ColumnVisibility.NodeType;
import org.apache.hadoop.io.Text;

public class VisibilityFlattener {
    public static ColumnVisibility flatten(Node root, byte[] expression, boolean sort) {
        StringBuilder out = new StringBuilder();
        flatten(root, expression, out, sort);
        return new ColumnVisibility(out.toString());
    }
    
    public static Text flattenToText(Node root, byte[] expression, boolean sort) {
        StringBuilder out = new StringBuilder();
        flatten(root, expression, out, sort);
        return new Text(out.toString());
    }
    
    private static void flatten(Node root, byte[] expression, StringBuilder out, boolean sort) {
        if (root.getType() == NodeType.TERM)
            out.append(new String(expression, root.getTermStart(), root.getTermEnd() - root.getTermStart(), UTF_8));
        else {
            String sep = "";
            Node[] children = root.getChildren().toArray(new Node[] {});
            if (sort)
                Arrays.sort(children, new NodeComparator(expression));
            for (Node c : children) {
                out.append(sep);
                boolean parens = (c.getType() != NodeType.TERM && root.getType() != c.getType());
                if (parens)
                    out.append("(");
                flatten(c, expression, out, sort);
                if (parens)
                    out.append(")");
                sep = root.getType() == NodeType.AND ? "&" : "|";
            }
        }
    }
}
