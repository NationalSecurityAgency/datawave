package datawave.query.language.parser.jexl;

import java.util.ArrayList;
import java.util.List;

public class JexlGroupingNode extends JexlNode {
    private JexlGroupingNode() {
        super(new ArrayList<>());
    }

    public JexlGroupingNode(List<JexlNode> children) {
        super(children);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        List<JexlNode> c = getChildren();

        sb.append("(");
        for (int x = 0; x < c.size(); x++) {
            JexlNode n = c.get(x);
            sb.append(n);
        }
        sb.append(")");

        return sb.toString();
    }
}
